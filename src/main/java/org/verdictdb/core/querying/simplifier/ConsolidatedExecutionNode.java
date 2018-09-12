package org.verdictdb.core.querying.simplifier;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;

/**
 * Used for simplifying two nodes into one. This class may be used in a recursively way to simplify
 * an arbitrary deep nodes into a single node (as long as the condition is satisfied).
 *
 * Assumptions:
 * <ol>
 * <li>
 * The token of a child node must not rely on its query results. Nodes like CreateTableAsSelect
 * is supposed to pass the name of a temporary table, which doesn't depend on the its query result.
 * </li>
 * </ol>
 * 
 * The logic based on the above assumption can be found in the createQuery() method.
 * Traditionally, createToken() must use the result of createQuery(); however, based on the
 * assumption, the token of the child is created without actually running its query. Note that the
 * child's select query is consolidated into the parent query as a subquery.
 *
 * @author Yongjoo Park
 */
public class ConsolidatedExecutionNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = -561220173745897906L;

  private QueryNodeWithPlaceHolders parentNode;

  private CreateTableAsSelectNode childNode;

  public ConsolidatedExecutionNode(
      SelectQuery selectQuery, QueryNodeWithPlaceHolders parent, CreateTableAsSelectNode child) {
    super(parent.getId(), selectQuery);
    parentNode = parent;
    childNode = child;

    //    for (PlaceHolderRecord record : parentNode.getPlaceholderRecords()) {
    //      addPlaceholderRecord(record);
    //    }
    //    for (PlaceHolderRecord record : childNode.getPlaceholderRecords()) {
    //      addPlaceholderRecord(record);
    //    }
  }

  @Override
  public PlaceHolderRecord removePlaceholderRecordForChannel(int channel) {
    PlaceHolderRecord record = parentNode.removePlaceholderRecordForChannel(channel);
    if (record != null) {
      return record;
    }
    record = childNode.removePlaceholderRecordForChannel(channel);
    return record;
  }

  public static ConsolidatedExecutionNode create(
      QueryNodeWithPlaceHolders parent, CreateTableAsSelectNode child) {

    SelectQuery parentQuery = parent.getSelectQuery();

    // find placeholders in the parent query and replace with the child query
    int childChannel = parent.getChannelForSource(child);
    PlaceHolderRecord placeHolderToRemove = parent.removePlaceholderRecordForChannel(childChannel);
    BaseTable baseTableToRemove = placeHolderToRemove.getPlaceholderTable();

    // replace in the source list
    parentQuery = (SelectQuery) consolidateSource(parentQuery, child, baseTableToRemove);

    // filter: replace the placeholder BaseTable (in the filter list) with
    // the SelectQuery of the child
    Optional<UnnamedColumn> parentFilterOptional = parentQuery.getFilter();
    if (parentFilterOptional.isPresent()) {
      UnnamedColumn originalFilter = parentFilterOptional.get();
      UnnamedColumn newFilter = consolidateFilter(originalFilter, child, baseTableToRemove);
      parentQuery.clearFilter();
      parentQuery.addFilterByAnd(newFilter);
    }

    ConsolidatedExecutionNode node =
        new ConsolidatedExecutionNode(parentQuery, parent, child);
    return node;
  }

  /**
   * May consonlidate a single source with `child`. This is a helper function for simplify2().
   *
   * @param originalSource The original source
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return A new source
   */
  private static AbstractRelation consolidateSource(
      AbstractRelation originalSource, ExecutableNodeBase child, BaseTable baseTableToRemove) {

    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalSource;
    }

    SelectQuery childQuery = ((QueryNodeBase) child).getSelectQuery();

    if (originalSource instanceof BaseTable) {
      BaseTable baseTableSource = (BaseTable) originalSource;
      if (baseTableSource.equals(baseTableToRemove)) {
        childQuery.setAliasName(baseTableToRemove.getAliasName().get());
        return childQuery;
      } else {
        return originalSource;
      }
    } else if (originalSource instanceof JoinTable) {
      JoinTable joinTableSource = (JoinTable) originalSource;
      List<AbstractRelation> joinSourceList = joinTableSource.getJoinList();
      List<AbstractRelation> newJoinSourceList = new ArrayList<>();
      for (AbstractRelation joinSource : joinSourceList) {
        newJoinSourceList.add(consolidateSource(joinSource, child, baseTableToRemove));
      }
      joinTableSource.setJoinList(newJoinSourceList);
      return joinTableSource;

    } else if (originalSource instanceof SelectQuery) {
      SelectQuery selectQuerySource = (SelectQuery) originalSource;
      List<AbstractRelation> originalFromList = selectQuerySource.getFromList();
      List<AbstractRelation> newFromList = new ArrayList<>();
      for (AbstractRelation source : originalFromList) {
        AbstractRelation newSource = consolidateSource(source, child, baseTableToRemove);
        newFromList.add(newSource);
      }
      selectQuerySource.setFromList(newFromList);
      return selectQuerySource;

    } else {
      return originalSource;
    }
  }

  /**
   * May consolidate a single filter with `child`. This is a helper function for simplify2().
   *
   * @param originalFilter The original filter
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return
   */
  private static UnnamedColumn consolidateFilter(
      UnnamedColumn originalFilter, ExecutableNodeBase child, BaseTable baseTableToRemove) {

    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalFilter;
    }

    SelectQuery childSelectQuery = ((QueryNodeBase) child).getSelectQuery();

    if (originalFilter instanceof ColumnOp) {
      ColumnOp originalColumnOp = (ColumnOp) originalFilter;
      List<UnnamedColumn> newOperands = new ArrayList<>();
      for (UnnamedColumn o : originalColumnOp.getOperands()) {
        newOperands.add(consolidateFilter(o, child, baseTableToRemove));
      }
      ColumnOp newColumnOp = new ColumnOp(originalColumnOp.getOpType(), newOperands);
      return newColumnOp;
    } else if (originalFilter instanceof SubqueryColumn) {
      SubqueryColumn originalSubquery = (SubqueryColumn) originalFilter;
      SelectQuery subquery = originalSubquery.getSubquery();
      List<AbstractRelation> subqueryFromList = subquery.getFromList();
      if (subqueryFromList.size() == 1 && subqueryFromList.get(0).equals(baseTableToRemove)) {
        originalSubquery.setSubquery(childSelectQuery);
      }
      return originalSubquery;
    }

    return originalFilter;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    // this will replace the placeholders contained the subquery.
    childNode.createQuery(tokens);
    ExecutionInfoToken childToken = childNode.createToken(null);

    // also pass the tokens possibly created by child.
    List<ExecutionInfoToken> newTokens = new ArrayList<>();
    newTokens.addAll(tokens);
    newTokens.add(childToken);
    
    // this is a bad trick to use the createQuery function of the parent node.
    // we set the consolidated query (of this node) to the parentNode, and let the parent node
    // create a new query using the consolidated select query.
    // for now, the only use case is that the parent node creates a "create table ..." query that
    // contains the consolidated select query.
    parentNode.setSelectQuery(selectQuery);
    SqlConvertible consolidatedQuery = parentNode.createQuery(newTokens);
    return consolidatedQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    // pass the information from the internal parent node
    ExecutionInfoToken token = parentNode.createToken(result);
    return token;
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("subscriberCount", getSubscribers().size())
        .append("sourceCount", getSources().size())
        .append("parent", parentNode)
        .append("child", childNode)
        .toString();
  }
}
