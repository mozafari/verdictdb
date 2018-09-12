/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying.simplifier;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.PlaceHolderRecord;
import org.verdictdb.core.querying.ProjectionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.QueryNodeWithPlaceHolders;
import org.verdictdb.core.querying.SelectAllExecutionNode;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBValidationException;

public class QueryExecutionPlanSimplifier {
  
  /**
   * Simplifies the originalPlan in place.
   *
   * The parent node may consolidate with its child when all of the following conditions are
   * satisfied:
   * 1. The child node is a descendant of CreateTableAsSelectNode
   * 2. The child node is the unique source of the channel to which the child node is set to
   *    broadcast
   * 3. The parent is the only subscriber of the child.
   * 4. ProjectionNode can be safely consolidated.
   * 5. AggExecutionNode can only be consolidated when its aggMeta is empty.
   *
   * @param originalPlan The plan to simplify
   * @throws VerdictDBValidationException This exception is thrown if the number of placeholders in
   * the parent does not match the number of the children.
   */
  public static void simplify2(QueryExecutionPlan originalPlan)
      throws VerdictDBValidationException {
    
    // first, simplify the root and its sources
    ExecutableNodeBase parent = originalPlan.getRootNode();
    ExecutableNodeBase newParent = simplify2ParentNode(parent);
    originalPlan.setRootNode(newParent);
    
    // next, simplify the sources
    // A triple below is (grandParent, parent); a consolidation may combine the parent 
    // and the child, which is set as the new child of the grandParent.
    List<Pair<ExecutableNodeBase, ExecutableNodeBase>> parentSourceList =
        new LinkedList<>();
    for (ExecutableNodeBase child : newParent.getSources()) {
      parentSourceList.add(Pair.of(newParent, child));
    }
    
    while (parentSourceList.size() > 0) {
      // take one candidate
      Pair<ExecutableNodeBase, ExecutableNodeBase> candidate = parentSourceList.remove(0);
      ExecutableNodeBase grandParent = candidate.getLeft();
      parent = candidate.getRight();
      
      // consolidates if possible
      newParent = simplify2ParentNode(parent);
      grandParent.replaceSource(parent, newParent);
      
      // add the (parent, child) as a next candidate
      for (ExecutableNodeBase child : newParent.getSources()) {
        parentSourceList.add(Pair.of(newParent, child));
      }
      
    }
  }
  
  /**
   * Consolidates a node to its children until possible. Note that this function may consolidate
   * one or more levels. This is a helper function for simplify2().
   * 
   * @param parent This parent node and its children will be simplified if available.
   * @return A new parent that will replace the passed parent. If the returned parent is identical
   *         to the passed parent, it means no consolidation occurred.
   * @throws VerdictDBValidationException 
   */
  private static ExecutableNodeBase simplify2ParentNode(ExecutableNodeBase parent) 
      throws VerdictDBValidationException {
    
    // Every iteration of this loop completely reconfigure placeholder list and subscription list
    // properly so that the next iteration does not need to know about the previous iteration.
    while (true) {
      List<ExecutableNodeBase> sources = parent.getSources();
  
      // if the parent has no child, this loop will immediately finish.
      // if any child is consolidated, this loop will start all over again from the beginning
      // however, since each consolidation removes a node from the tree, this loop only iterates
      // as many times as the number of the nodes in the tree.
      ExecutableNodeBase newParent = null;
      for (int childIndex = 0; childIndex < sources.size(); childIndex++) {
        newParent = consolidates(parent, childIndex);
        if (newParent != null) {
          break;
        }
      }
      if (newParent == null) {
        break;
      } else {
        parent = newParent;
      }
    }
    
    return parent;
  }
  
  /**
   * Consolidates to a single child if the condition is met. This is a helper function for
   * simplify2().
   *
   * @param parent The parent node
   * @param childIndex The index of the child node to consolidate (if possible)
   * @return True if consolidated; false otherwise
   * @throws VerdictDBValidationException This exception is thrown if the number of placeholders in
   * the parent does not match the number of the children.
   */
  private static ExecutableNodeBase consolidates(ExecutableNodeBase parent, int childIndex)
      throws VerdictDBValidationException {
  
    List<ExecutableNodeBase> sources = parent.getSources();
    ExecutableNodeBase child = sources.get(childIndex);
  
    // Check consolidation conditions
    
    // first condition: the child must inherits CreateTableAsSelectNode.
    if (!(child instanceof CreateTableAsSelectNode)) {
      return null;
    }
    
    // second condition: the child must be the unique broadcaster to the channel it broadcasts to.
    int channelSharingSourceCount = 0;
    int childChannel = parent.getChannelForSource(child);
    List<Pair<ExecutableNodeBase, Integer>> sourceAndChannelList = parent.getSourcesAndChannels();
    for (Pair<ExecutableNodeBase, Integer> sourceAndChannel : sourceAndChannelList) {
      int channel = sourceAndChannel.getRight();
      if (channel == childChannel) {
        channelSharingSourceCount += 1;
      }
    }
    if (channelSharingSourceCount > 1) {
      return null;
    }
    
    // third condition: the parent is the only subscriber of the child
    if (child.getSubscribers().size() > 1) {
      return null;
    }

    // fourth condition: cannot simplify if a AsyncAggExecutionNode (i.e., child)'s parent has 
    // another AsyncAggExecutionNode among its children.
    if (child instanceof AsyncAggExecutionNode) {
      for (ExecutableNodeBase source : parent.getSources()) {
        if (!source.equals(child) && source instanceof AsyncAggExecutionNode) {
          return null;
        }
      }
    }
    
    // Now actually consolidate
    ExecutableNodeBase newParent = null;
    if ((parent instanceof SelectAllExecutionNode ||
        parent instanceof ConsolidatedExecutionNode ||
        parent instanceof AggExecutionNode) && 
        (child instanceof AsyncAggExecutionNode
            || child instanceof AggExecutionNode
            || child instanceof ProjectionNode)) {
      newParent = ConsolidatedExecutionNode.create(
          (QueryNodeWithPlaceHolders) parent, (CreateTableAsSelectNode) child);
    }
    
    if (newParent == null) {
      return null;
    }
    
    // the parent's subscription to the child is removed.
    // however, the parent now subscribes to the previous broadcastors to the child.
    parent.cancelSubscriptionTo(child);
    List<Pair<ExecutableNodeBase, Integer>> childSourceAndChannels = child.getSourcesAndChannels();
    for (Pair<ExecutableNodeBase, Integer> childSourceAndChannel : childSourceAndChannels) {
      ExecutableNodeBase childSource = childSourceAndChannel.getLeft();
      int childSourceChannel = childSourceAndChannel.getRight();
      child.cancelSubscriptionTo(childSource);
      newParent.subscribeTo(childSource, childSourceChannel);
    }
    
    // copy the old parent's subscriptions under the new parent
    for (Pair<ExecutableNodeBase, Integer> sourceAndChannel : parent.getSourcesAndChannels()) {
      ExecutableNodeBase source = sourceAndChannel.getLeft();
      int channel = sourceAndChannel.getRight();
      parent.cancelSubscriptionTo(source);
      newParent.subscribeTo(source, channel);
    }
    
    // let the parents of the old parent subscribe instead to the new parent
    List<ExecutableNode> subscribers = parent.getSubscribers();
    for (ExecutableNode n : subscribers) {
      ((ExecutableNodeBase) n).replaceSource(parent, newParent);
    }
    
    
    // One extra step: if the root node is the "select *" query without any group-by clauses
    // we just use the inner query.
    if (newParent instanceof QueryNodeBase) {
      QueryNodeBase parentAsQueryNode = (QueryNodeBase) newParent;
      SelectQuery parentSelectQuery = parentAsQueryNode.getSelectQuery();
      List<SelectItem> parentSelectList = parentSelectQuery.getSelectList();
      List<AbstractRelation> parentFromList = parentSelectQuery.getFromList();
      List<GroupingAttribute> parentGroupbyList = parentSelectQuery.getGroupby();
      if (parentSelectList.size() == 1
              && (parentSelectList.get(0) instanceof AsteriskColumn)
              && (parentGroupbyList.size() == 0)
              && (parentFromList.size() == 1)
              && (parentFromList.get(0) instanceof SelectQuery)) {
        SelectQuery innerQuery = (SelectQuery) parentFromList.get(0);
        innerQuery.clearAliasName();
        parentAsQueryNode.setSelectQuery(innerQuery);
      }
    }
    
    // returns the new parent
    return newParent;
  }
  
  
  

  /**
   * @param originalPlan
   * @return a deepcopy of plan which is simplified
   */
  public static QueryExecutionPlan simplify(QueryExecutionPlan originalPlan) {
    // Deep copy originalPlan
    QueryExecutionPlan plan = originalPlan.deepcopy();

    List<ExecutableNodeBase> nodesToCompress = new ArrayList<>();
    // compress the node from bottom to up in order to replace the select query conveniently
    List<ExecutableNodeBase> traverse = new ArrayList<>();
    traverse.add(plan.getRoot());
    while (!traverse.isEmpty()) {
      ExecutableNodeBase node = traverse.get(0);
      traverse.remove(0);
      if (node.getDependentNodeCount() == 0 && !nodesToCompress.contains(node)) {
        nodesToCompress.add(node);
      } else traverse.addAll(node.getExecutableNodeBaseDependents());
    }

    List<ExecutableNodeBase> history = new ArrayList<>();
    while (!nodesToCompress.isEmpty()) {
      ExecutableNodeBase node = nodesToCompress.remove(0);
      List<ExecutableNodeBase> nodeParentsSaved =
          new ArrayList<>(node.getExecutableNodeBaseParents());

      // Exception 1: has no parent(root), or has multiple parent
      // Exception 2: its parents has multiple dependents and this node share same queue with other
      // dependents
      // Exception 3: two nodes are not SelectAllNode, ProjectionNode or AggregateNode
      boolean compressable =
          node.getExecutableNodeBaseParents().size() == 1 && !isSharingQueue(node);
      if (compressable) {
        ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
        if (((parent instanceof AggExecutionNode)
                || (parent instanceof SelectAllExecutionNode)
                || (parent instanceof ProjectionNode && !(parent instanceof AsyncAggExecutionNode)))
            && ((node instanceof AggExecutionNode)
                || (node instanceof SelectAllExecutionNode)
                || (node instanceof ProjectionNode && !(node instanceof AsyncAggExecutionNode)))) {
          compressTwoNode(node, parent);
        }
      }
      history.add(node);

      // the parent information of the "node" has been removed
      for (ExecutableNodeBase parent : nodeParentsSaved) {
        if (!history.contains(parent) && !nodesToCompress.contains(parent)) {
          nodesToCompress.add(parent);
        }
      }
    }
    return plan;
  }

  // Compress node and parent into parent, node will be useless
  static void compressTwoNode(ExecutableNodeBase node, ExecutableNodeBase parent) {
    if (!(node instanceof QueryNodeBase) || !(parent instanceof QueryNodeBase)) {
      return;
    }
    QueryNodeBase parentQuery = (QueryNodeBase) parent;
    QueryNodeBase nodeQuery = (QueryNodeBase) node;

    // Change the query of parents
    PlaceHolderRecord placeholderRecordinParent =
        ((QueryNodeWithPlaceHolders) parent)
            .getPlaceholderRecords()
            .get(parent.getExecutableNodeBaseDependents().indexOf(node));
    ((QueryNodeWithPlaceHolders) parent).getPlaceholderRecords().remove(placeholderRecordinParent);
    BaseTable placeholderTableinParent =placeholderRecordinParent.getPlaceholderTable();
//    BaseTable placeholderTableinParent =
//        ((QueryNodeWithPlaceHolders) parent)
//            .getPlaceholderTables()
//            .get(parent.getExecutableNodeBaseDependents().indexOf(node));
//    ((QueryNodeWithPlaceHolders) parent).getPlaceholderTables().remove(placeholderTableinParent);

    // If temp table is in from list of parent, just direct replace with the select query of node
    boolean find = false;
    for (AbstractRelation table : parentQuery.getSelectQuery().getFromList()) {
      if (table instanceof BaseTable && table.equals(placeholderTableinParent)) {
        int index = parentQuery.getSelectQuery().getFromList().indexOf(table);
        nodeQuery
            .getSelectQuery()
            .setAliasName(
                parentQuery.getSelectQuery().getFromList().get(index).getAliasName().get());
        parentQuery.getSelectQuery().getFromList().set(index, nodeQuery.getSelectQuery());
        find = true;
        break;
      } else if (table instanceof JoinTable) {
        for (AbstractRelation joinTable : ((JoinTable) table).getJoinList()) {
          if (joinTable instanceof BaseTable && joinTable.equals(placeholderTableinParent)) {
            int index = ((JoinTable) table).getJoinList().indexOf(joinTable);
            nodeQuery.getSelectQuery().setAliasName(joinTable.getAliasName().get());
            ((JoinTable) table).getJoinList().set(index, nodeQuery.getSelectQuery());
            find = true;
            break;
          }
        }
        if (find) break;
      }
    }

    // Otherwise, it need to search filter to find the temp table
    if (!find) {
      List<SubqueryColumn> placeholderTablesinFilter =
          ((QueryNodeWithPlaceHolders) parent).getPlaceholderTablesinFilter();
      for (SubqueryColumn filter : placeholderTablesinFilter) {
        if (filter.getSubquery().getFromList().size() == 1
            && filter.getSubquery().getFromList().get(0).equals(placeholderTableinParent)) {
          filter.setSubquery(nodeQuery.getSelectQuery());
        }
      }
    }

    // Move node's placeholderTable to parent's
    ((QueryNodeWithPlaceHolders) parent)
        .getPlaceholderRecords().addAll(((QueryNodeWithPlaceHolders) node).getPlaceholderRecords());
//    ((QueryNodeWithPlaceHolders) parent)
//        .getPlaceholderTables().addAll(((QueryNodeWithPlaceHolders) node).getPlaceholderTables());

    // Compress the node tree
    parentQuery.cancelSubscriptionTo(nodeQuery);
    for (Pair<ExecutableNodeBase, Integer> s : nodeQuery.getSourcesAndChannels()) {
      parentQuery.subscribeTo(s.getLeft(), s.getRight());
    }
    //    parent.getListeningQueues().removeAll(node.broadcastingQueues);
    //    parent.getListeningQueues().addAll(node.getListeningQueues());
    //    parent.dependents.remove(node);
    //    parent.dependents.addAll(node.dependents);
    //    for (BaseQueryNode dependent:node.dependents) {
    //      dependent.parents.remove(node);
    //      dependent.parents.add(parent);
    //    }
  }

  /**
   *
   * @param node
   * @return true if node is sharing channel with other sources of its subsriber
   */
   static boolean isSharingQueue(ExecutableNodeBase node) {
    // must have one parent and this parent must have multiple dependents
    if (node.getExecutableNodeBaseParents().size() != 1
        || node.getExecutableNodeBaseParents().get(0).getDependentNodeCount() <= 1) {
      return false;
    }
    else {
      ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
      int nodeIndex = parent.getSources().indexOf(node);
      for (ExecutableNodeBase dependent : parent.getExecutableNodeBaseDependents()) {
        if (!dependent.equals(node)) {
          int dependentIndex = parent.getSources().indexOf(dependent);
          if (parent.getSourcesAndChannels().get(nodeIndex).getRight().equals(
              parent.getSourcesAndChannels().get(dependentIndex).getRight())) {
            return true;
          }
        }
      }
      return false;
    }
  }
}
