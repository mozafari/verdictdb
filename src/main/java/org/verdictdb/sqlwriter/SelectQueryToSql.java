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

package org.verdictdb.sqlwriter;

import java.util.List;
import java.util.Set;

import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SetOperationRelation;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

public class SelectQueryToSql {

  SqlSyntax syntax;

  Set<String> opTypeNotRequiringParentheses =
      Sets.newHashSet(
          "sum",
          "avg",
          "count",
          "max",
          "min",
          "std",
          "sqrt",
          "is_not_null",
          "is_null",
          "rand",
          "floor");

  public SelectQueryToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(AbstractRelation relation) throws VerdictDBException {
    if (!(relation instanceof SelectQuery)) {
      throw new VerdictDBTypeException("Unexpected type: " + relation.getClass().toString());
    }
    return selectQueryToSql((SelectQuery) relation);
  }

  String selectItemToSqlPart(SelectItem item) throws VerdictDBException {
    if (item instanceof AliasedColumn) {
      return aliasedColumnToSqlPart((AliasedColumn) item);
    } else if (item instanceof UnnamedColumn) {
      return unnamedColumnToSqlPart((UnnamedColumn) item);
    } else {
      throw new VerdictDBTypeException("Unexpceted argument type: " + item.getClass().toString());
    }
  }

  String aliasedColumnToSqlPart(AliasedColumn acolumn) throws VerdictDBException {
    String aliasName = acolumn.getAliasName();
    return unnamedColumnToSqlPart(acolumn.getColumn()) + " as " + quoteName(aliasName);
  }

  String groupingAttributeToSqlPart(GroupingAttribute column) throws VerdictDBException {
    if (column instanceof AsteriskColumn) {
      throw new VerdictDBTypeException("asterisk is not expected in the groupby clause.");
    }
    if (column instanceof AliasReference) {
      AliasReference aliasedColumn = (AliasReference) column;
      return (aliasedColumn.getTableAlias() != null)
          ? quoteName(aliasedColumn.getTableAlias()) + "." +
                quoteName(aliasedColumn.getAliasName())
          : quoteName(aliasedColumn.getAliasName());
    } else {
      return unnamedColumnToSqlPart((UnnamedColumn) column);
    }
  }

  String unnamedColumnToSqlPart(UnnamedColumn column) throws VerdictDBException {
    if (column instanceof BaseColumn) {
      BaseColumn base = (BaseColumn) column;
      if (base.getTableSourceAlias() == null) {
        return base.getColumnName();
      }
      if (base.getTableSourceAlias().equals("")) {
        return quoteName(base.getColumnName());
      } else return base.getTableSourceAlias() + "." + quoteName(base.getColumnName());
    } else if (column instanceof ConstantColumn) {
      return ((ConstantColumn) column).getValue().toString();
    } else if (column instanceof AsteriskColumn) {
      return "*";
    } else if (column instanceof ColumnOp) {
      ColumnOp columnOp = (ColumnOp) column;
      if (columnOp.getOpType().equals("avg")) {
        return "avg(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("sum")) {
        return "sum(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("count")) {
        if (columnOp.getOperands().isEmpty()) return "count(*)";
        else return "count(" + unnamedColumnToSqlPart(columnOp.getOperand(0)) + ")";
      } else if (columnOp.getOpType().equals("stddev_pop")) {
        String stddevPopulationFunctionName = syntax.getStddevPopulationFunctionName();
        return String.format("%s(", stddevPopulationFunctionName)
            + unnamedColumnToSqlPart(columnOp.getOperand())
            + ")";
      } else if (columnOp.getOpType().equals("sqrt")) {
        return "sqrt(" + unnamedColumnToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("add")) {
        return withParentheses(columnOp.getOperand(0))
            + " + "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("subtract")) {
        return withParentheses(columnOp.getOperand(0))
            + " - "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("multiply")) {
        return withParentheses(columnOp.getOperand(0))
            + " * "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("divide")) {
        return withParentheses(columnOp.getOperand(0))
            + " / "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("pow")) {
        return "pow("
            + unnamedColumnToSqlPart(columnOp.getOperand(0))
            + ", "
            + unnamedColumnToSqlPart(columnOp.getOperand(1))
            + ")";
      } else if (columnOp.getOpType().equals("equal")) {
        return withParentheses(columnOp.getOperand(0))
            + " = "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("and")) {
        return withParentheses(columnOp.getOperand(0))
            + " and "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("or")) {
        return withParentheses(columnOp.getOperand(0))
            + " or "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("not")) {
        return "not " + withParentheses(columnOp.getOperand(0));
      } else if (columnOp.getOpType().equals("casewhen")) {
        StringBuilder sql = new StringBuilder();
        sql.append("case");
        for (int i = 0; i < columnOp.getOperands().size() - 1; i = i + 2) {
          sql.append(" when " + withParentheses(columnOp.getOperand(i))
                         + " then " + withParentheses(columnOp.getOperand(i + 1)));
        }
        sql.append(
            " else " + withParentheses(columnOp.getOperand(columnOp.getOperands().size() - 1))
                       + " end");
        return sql.toString();
      } else if (columnOp.getOpType().equals("notequal")) {
        return withParentheses(columnOp.getOperand(0))
            + " <> "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("notand")) {
        return "not ("
            + withParentheses(columnOp.getOperand(0))
            + " and "
            + withParentheses(columnOp.getOperand(1))
            + ")";
      } else if (columnOp.getOpType().equals("isnull")) {
        return "isnull(" + withParentheses(columnOp.getOperand(0)) + ")";
      } else if (columnOp.getOpType().equals("is_null")) {
        return withParentheses(columnOp.getOperand(0)) + " is null";
      } else if (columnOp.getOpType().equals("is_not_null")) {
        return withParentheses(columnOp.getOperand(0)) + " is not null";
      } else if (columnOp.getOpType().equals("interval")) {
        return "interval "
            + withParentheses(columnOp.getOperand(0))
            + " "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("date")) {
        return "date " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("greater")) {
        return withParentheses(columnOp.getOperand(0))
            + " > "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("less")) {
        return withParentheses(columnOp.getOperand(0))
            + " < "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("greaterequal")) {
        return withParentheses(columnOp.getOperand(0))
            + " >= "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("lessequal")) {
        return withParentheses(columnOp.getOperand(0))
            + " <= "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("min")) {
        return "min(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("max")) {
        return "max(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("min")) {
        return "max(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("floor")) {
        return "floor(" + selectItemToSqlPart(columnOp.getOperand()) + ")";
        //      } else if (columnOp.getOpType().equals("is")) {
        //        return withParentheses(columnOp.getOperand(0)) + " is " +
        // withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("like")) {
        return withParentheses(columnOp.getOperand(0))
            + " like "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("notlike")) {
        return withParentheses(columnOp.getOperand(0))
            + " not like "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("rlike")) {
        return withParentheses(columnOp.getOperand(0))
            + " rlike "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("notrlike")) {
        return withParentheses(columnOp.getOperand(0))
            + " not rlike "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("exists")) {
        return "exists " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("notexists")) {
        return "not exists " + withParentheses(columnOp.getOperand());
      } else if (columnOp.getOpType().equals("between")) {
        return withParentheses(columnOp.getOperand(0))
            + " between "
            + withParentheses(columnOp.getOperand(1))
            + " and "
            + withParentheses(columnOp.getOperand(2));
      } else if (columnOp.getOpType().equals("in")) {
        List<UnnamedColumn> columns = columnOp.getOperands();
        if (columns.size() == 2 && columns.get(1) instanceof SubqueryColumn) {
          return withParentheses(columns.get(0)) + " in " + withParentheses(columns.get(1));
        }
        String temp = "";
        for (int i = 1; i < columns.size(); i++) {
          if (i != columns.size() - 1) {
            temp = temp + withParentheses(columns.get(i)) + ", ";
          } else temp = temp + withParentheses(columns.get(i));
        }
        return withParentheses(columns.get(0)) + " in (" + temp + ")";
      } else if (columnOp.getOpType().equals("notin")) {
        List<UnnamedColumn> columns = columnOp.getOperands();
        if (columns.size() == 2 && columns.get(1) instanceof SubqueryColumn) {
          return withParentheses(columns.get(0)) + " not in " + withParentheses(columns.get(1));
        }
        String temp = "";
        for (int i = 1; i < columns.size(); i++) {
          if (i != columns.size() - 1) {
            temp = temp + withParentheses(columns.get(i)) + ", ";
          } else temp = temp + withParentheses(columns.get(i));
        }
        return withParentheses(columns.get(0)) + " not in (" + temp + ")";
      } else if (columnOp.getOpType().equals("countdistinct")) {
        return "count(distinct " + withParentheses(columnOp.getOperand()) + ")";
      } else if (columnOp.getOpType().equals("approx_countdistinct")) {
        return syntax.getApproximateCountDistinct(withParentheses(columnOp.getOperand()));
      } else if (columnOp.getOpType().equals("substr")) {
        if (columnOp.getOperands().size() == 3) {
          return "substr("
              + withParentheses(columnOp.getOperand(0))
              + ", "
              + withParentheses(columnOp.getOperand(1))
              + ", "
              + withParentheses(columnOp.getOperand(2))
              + ")";
        } else {
          return "substr("
              + withParentheses(columnOp.getOperand(0))
              + ", "
              + withParentheses(columnOp.getOperand(1))
              + ")";
        }
      } else if (columnOp.getOpType().equals("rand")) {
        return syntax.randFunction();
      } else if (columnOp.getOpType().equals("cast")) {
        // MySQL cast as int should be replaced by cast as unsigned
        if (syntax instanceof MysqlSyntax
            && columnOp.getOperand(1) instanceof ConstantColumn
            && ((ConstantColumn) columnOp.getOperand(1)).getValue().toString().equals("int")) {
          return "cast("
              + withParentheses(columnOp.getOperand(0))
              + " as "
              + "unsigned"
              + ")";
        }
        else return "cast("
            + withParentheses(columnOp.getOperand(0))
            + " as "
            + withParentheses(columnOp.getOperand(1))
            + ")";
      } else if (columnOp.getOpType().equals("extract")) {
        return "extract("
            + withParentheses(columnOp.getOperand(0))
            + " from "
            + withParentheses(columnOp.getOperand(1))
            + ")";
      } else if (columnOp.getOpType().equals("||")
          || columnOp.getOpType().equals("|")
          || columnOp.getOpType().equals("&")
          || columnOp.getOpType().equals("#")
          || columnOp.getOpType().equals(">>")
          || columnOp.getOpType().equals("<<")) {
        return withParentheses(columnOp.getOperand(0))
            + " "
            + columnOp.getOpType()
            + " "
            + withParentheses(columnOp.getOperand(1));
      } else if (columnOp.getOpType().equals("overlay")) {
        return "overlay("
            + withParentheses(columnOp.getOperand(0))
            + " placing "
            + withParentheses(columnOp.getOperand(1))
            + " from "
            + withParentheses(columnOp.getOperand(2))
            + ")";
      } else if (columnOp.getOpType().equals("substring") && (syntax instanceof PostgresqlSyntax)) {
        String temp =
            "substring("
                + withParentheses(columnOp.getOperand(0))
                + " from "
                + withParentheses(columnOp.getOperand(1));
        if (columnOp.getOperands().size() == 2) {
          return temp + ")";
        } else {
          return temp + " for " + withParentheses(columnOp.getOperand(2)) + ")";
        }
      } else if (columnOp.getOpType().equals("timestampwithoutparentheses")) {
        return "timestamp " + withParentheses(columnOp.getOperand(0));
      } else if (columnOp.getOpType().equals("dateadd")) {
        return "dateadd("
            + withParentheses(columnOp.getOperand(0))
            + ", "
            + withParentheses(columnOp.getOperand(1))
            + ", "
            + withParentheses(columnOp.getOperand(2))
            + ")";
      } else {
        List<UnnamedColumn> columns = columnOp.getOperands();
        String temp = columnOp.getOpType() + "(";
        for (int i = 0; i < columns.size(); i++) {
          if (i != columns.size() - 1) {
            temp = temp + withParentheses(columns.get(i)) + ", ";
          } else temp = temp + withParentheses(columns.get(i));
        }
        return temp + ")";
      }
      // else {
      //  throw new VerdictDBTypeException("Unexpceted opType of column: " +
      // columnOp.getOpType().toString());
      // }
    } else if (column instanceof SubqueryColumn) {
      return "(" + selectQueryToSql(((SubqueryColumn) column).getSubquery()) + ")";
    } else if (column instanceof AliasReference) {
      return groupingAttributeToSqlPart(column);
    }
    throw new VerdictDBTypeException("Unexpceted argument type: " + column.getClass().toString());
  }

  String withParentheses(UnnamedColumn column) throws VerdictDBException {
    String sql = unnamedColumnToSqlPart(column);
    if (column instanceof ColumnOp
        && !opTypeNotRequiringParentheses.contains(((ColumnOp) column).getOpType())) {
      sql = "(" + sql + ")";
    }
    return sql;
  }

  String selectQueryToSql(SelectQuery sel) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    // select
    sql.append("select");
    List<SelectItem> columns = sel.getSelectList();
    boolean isFirstColumn = true;
    for (SelectItem a : columns) {
      if (isFirstColumn) {
        sql.append(" " + selectItemToSqlPart(a));
        isFirstColumn = false;
      } else {
        sql.append(", " + selectItemToSqlPart(a));
      }
    }

    // from
    List<AbstractRelation> rels = sel.getFromList();
    if (rels.size() > 0) {
      sql.append(" from");
      boolean isFirstRel = true;
      for (AbstractRelation r : rels) {
        if (isFirstRel) {
          sql.append(" " + relationToSqlPart(r));
          isFirstRel = false;
        } else {
          sql.append(", " + relationToSqlPart(r));
        }
      }
    }

    // where
    Optional<UnnamedColumn> filter = sel.getFilter();
    if (filter.isPresent()) {
      sql.append(" where ");
      sql.append(unnamedColumnToSqlPart(filter.get()));
    }

    // groupby
    List<GroupingAttribute> groupby = sel.getGroupby();
    boolean isFirstGroup = true;
    for (GroupingAttribute a : groupby) {
      if (isFirstGroup) {
        sql.append(" group by ");
        sql.append(groupingAttributeToSqlPart(a));
        isFirstGroup = false;
      } else {
        sql.append(", " + groupingAttributeToSqlPart(a));
      }
    }

    // having
    Optional<UnnamedColumn> having = sel.getHaving();
    if (having.isPresent()) {
      sql.append(" having ");
      sql.append(unnamedColumnToSqlPart(having.get()));
    }

    // orderby
    List<OrderbyAttribute> orderby = sel.getOrderby();
    boolean isFirstOrderby = true;
    for (OrderbyAttribute a : orderby) {
      if (isFirstOrderby) {
        sql.append(" order by ");
        sql.append(groupingAttributeToSqlPart(a.getAttribute()));
        sql.append(" " + a.getOrder());
        if (!a.getNullsOrder().isEmpty()) {
          sql.append(" " + a.getNullsOrder());
        }
        isFirstOrderby = false;
      } else {
        sql.append(", " + groupingAttributeToSqlPart(a.getAttribute()));
        sql.append(" " + a.getOrder());
        if (!a.getNullsOrder().isEmpty()) {
          sql.append(" " + a.getNullsOrder());
        }
      }
    }

    // Limit
    Optional<UnnamedColumn> limit = sel.getLimit();
    if (limit.isPresent()) {
      sql.append(" limit " + unnamedColumnToSqlPart(limit.get()));
    }

    return sql.toString();
  }

  String relationToSqlPart(AbstractRelation relation) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    if (relation instanceof BaseTable) {
      BaseTable base = (BaseTable) relation;
      if (base.getSchemaName().isEmpty()) {
        sql.append(quoteName(base.getTableName()));
      } else {
        sql.append(quoteName(base.getSchemaName()) + "." + quoteName(base.getTableName()));
      }
      if (base.getAliasName().isPresent()) {
        sql.append(" as " + base.getAliasName().get());
      }
      return sql.toString();
    }

    if (relation instanceof JoinTable) {
      // sql.append("(");
      sql.append(relationToSqlPart(((JoinTable) relation).getJoinList().get(0)));
      for (int i = 1; i < ((JoinTable) relation).getJoinList().size(); i++) {
        if (((JoinTable) relation).getJoinTypeList().get(i - 1).equals(JoinTable.JoinType.inner)) {
          sql.append(" inner join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.outer)) {
          sql.append(" outer join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.left)) {
          sql.append(" left join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.leftouter)) {
          sql.append(" left outer join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.right)) {
          sql.append(" right join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.rightouter)) {
          sql.append(" right outer join ");
        } else if (((JoinTable) relation)
            .getJoinTypeList()
            .get(i - 1)
            .equals(JoinTable.JoinType.cross)) {
          sql.append(" cross join ");
        }
        sql.append(relationToSqlPart(((JoinTable) relation).getJoinList().get(i)));
        if (((JoinTable) relation).getCondition().get(i - 1) != null)
          sql.append(" on " + withParentheses(((JoinTable) relation).getCondition().get(i - 1)));
      }
      // sql.append(")");
      if (((JoinTable) relation).getAliasName().isPresent()) {
        sql.append(" as " + ((JoinTable) relation).getAliasName().toString());
      }
      return sql.toString();
    }
    if (relation instanceof SetOperationRelation) {
      sql.append("(");
      sql.append(selectQueryToSql((SelectQuery) ((SetOperationRelation) relation).getLeft()));
      sql.append(" ");
      sql.append(((SetOperationRelation) relation).getSetOpType());
      sql.append(" ");
      sql.append(selectQueryToSql((SelectQuery) ((SetOperationRelation) relation).getRight()));
      sql.append(")");
      if (relation.getAliasName().isPresent()) {
        sql.append(" as " + relation.getAliasName().get());
      }
      return sql.toString();
    }

    if (!(relation instanceof SelectQuery)) {
      throw new VerdictDBTypeException(
          "Unexpected relation type: " + relation.getClass().toString());
    }

    SelectQuery sel = (SelectQuery) relation;
    Optional<String> aliasName = sel.getAliasName();
    if (!aliasName.isPresent()) {
      throw new VerdictDBValueException("An inner select query must be aliased.");
    }

    return "(" + selectQueryToSql(sel) + ") as " + aliasName.get();
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    // already quoted
    if (name.startsWith(quoteString) && name.endsWith(quoteString)) {
      return name;
    } else return quoteString + name + quoteString;
  }
}
