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

package org.verdictdb.sqlreader;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.sqlobject.AbstractRelation;
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
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;

public class RelationGen extends VerdictSQLParserBaseVisitor<AbstractRelation> {

  //  private MetaData meta;

  private List<SelectItem> selectElems = new ArrayList<>();

  //  public RelationGen(MetaData meta) {
  //    this.meta = meta;
  //  }

  public RelationGen() {}

  @Override
  public SelectQuery visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
    SelectQuery sel = (SelectQuery) visit(ctx.query_expression());

    if (ctx.order_by_clause() != null) {
      for (VerdictSQLParser.Order_by_expressionContext o :
          ctx.order_by_clause().order_by_expression()) {
        ExpressionGen g = new ExpressionGen();
        UnnamedColumn c = g.visit(o.expression());
        String nullsOrder = "";
        if (o.NULLS() != null) {
          nullsOrder = (o.FIRST() != null) ? "nulls first" : "nulls last";
        }
        OrderbyAttribute orderbyCol =
            new OrderbyAttribute(c, (o.DESC() == null) ? "asc" : "desc", nullsOrder);
        sel.addOrderby(orderbyCol);
      }
    }

    if (ctx.limit_clause() != null) {
      sel.addLimit(ConstantColumn.valueOf(ctx.limit_clause().number().getText()));
    }
    return sel;
  }

  @Override
  public AbstractRelation visitQuery_expression(VerdictSQLParser.Query_expressionContext ctx) {
    AbstractRelation r = null;
    if (ctx.query_specification() != null) {
      r = this.visit(ctx.query_specification());
    } else if (ctx.query_expression() != null) {
      r = this.visit(ctx.query_expression());
    }
    /*
       for (VerdictSQLParser.UnionContext union : ctx.union()) {
           AbstractRelation other = this.visit(union);
           SetRelation.SetType type;
           if (union.UNION() != null) {
               type = SetRelation.SetType.UNION;
               if (union.ALL() != null) {
                   type = SetRelation.SetType.UNION_ALL;
               }
           } else if (union.EXCEPT() != null) {
               type = SetRelation.SetType.EXCEPT;
           } else if (union.INTERSECT() != null) {
               type = SetRelation.SetType.INTERSECT;
           } else {
               type = SetRelation.SetType.UNKNOWN;
           }
           r = new SetRelation(vc, r, other, type);
       }
    */
    return r;
  }

  /**
   * Parses a depth-one select statement. If there exist subqueries, this function will be called
   * recursively.
   */
  @Override
  public AbstractRelation visitQuery_specification(
      VerdictSQLParser.Query_specificationContext ctx) {

    class SelectListExtractor extends VerdictSQLParserBaseVisitor<List<SelectItem>> {
      @Override
      public List<SelectItem> visitSelect_list(VerdictSQLParser.Select_listContext ctx) {
        List<SelectItem> selectList = new ArrayList<>();
        for (VerdictSQLParser.Select_list_elemContext a : ctx.select_list_elem()) {
          VerdictSQLParserBaseVisitor<SelectItem> v =
              new VerdictSQLParserBaseVisitor<SelectItem>() {
                @Override
                public SelectItem visitSelect_list_elem(
                    VerdictSQLParser.Select_list_elemContext ctx) {
                  SelectItem elem = null;
                  if (ctx.STAR() != null) {
                    if (ctx.table_name() == null) {
                      elem = new AsteriskColumn();
                    } else {
                      elem = new AsteriskColumn(stripQuote(ctx.table_name().getText()));
                    }
                  } else {
                    ExpressionGen g = new ExpressionGen();
                    elem = g.visit(ctx.expression());
                    if (elem instanceof BaseColumn) {
                      if (ctx.column_alias() != null) {
                        elem = new AliasedColumn((BaseColumn) elem, stripQuote(ctx.column_alias().getText()));
                      }
                    } else if (elem instanceof ColumnOp) {
                      if (ctx.column_alias() != null) {
                        elem = new AliasedColumn((ColumnOp) elem, stripQuote(ctx.column_alias().getText()));
                      }
                    } else if (elem instanceof ConstantColumn) {
                      if (ctx.column_alias() != null) {
                        elem =
                            new AliasedColumn((ConstantColumn) elem, stripQuote(ctx.column_alias().getText()));
                      }
                    }
                  }
                  return elem;
                }
              };
          SelectItem e = v.visit(a);
          selectList.add(e);
        }
        return selectList;
      }
    }

    List<AbstractRelation> tableSources =
        new ArrayList<>(); // assume that only the first entry can be JoinedRelation
    for (VerdictSQLParser.Table_sourceContext s : ctx.table_source()) {
      AbstractRelation r1 = this.visit(s);
      tableSources.add(r1);
    }
    // parse the where clause;
    UnnamedColumn where = null;
    if (ctx.WHERE() != null) {
      CondGen g = new CondGen();
      where = g.visit(ctx.where);
    }

    // parse select list
    SelectListExtractor select = new SelectListExtractor();
    List<SelectItem> elems = select.visit(ctx.select_list());

    selectElems = elems; // used in visitSelect_statement()

    SelectQuery sel = SelectQuery.create(selectElems, tableSources);
    if (where != null) {
      sel.addFilterByAnd(where);
    }

    if (ctx.GROUP() != null) {
      List<GroupingAttribute> groupby = new ArrayList<GroupingAttribute>();
      for (VerdictSQLParser.Group_by_itemContext g : ctx.group_by_item()) {
        GroupingAttribute gexpr = null;
        ExpressionGen expressionGen = new ExpressionGen();
        UnnamedColumn c = expressionGen.visit(g);
        gexpr = c;
        boolean aliasFound = false;
        if (!aliasFound) {
          groupby.add(gexpr);
        }
      }
      if (!groupby.isEmpty()) {
        sel.addGroupby(groupby);
      }
    }

    UnnamedColumn having = null;
    if (ctx.HAVING() != null) {
      CondGen g = new CondGen();
      having = g.visit(ctx.having);
    }
    if (having != null) {
      sel.addHavingByAnd(having);
    }
    if (ctx.top_clause() != null) {
      sel.addLimit(ConstantColumn.valueOf(ctx.top_clause().number().getText()));
    }
    return sel;
  }

  private UnnamedColumn joinCond = null;

  private JoinTable.JoinType joinType = null;

  @Override
  public AbstractRelation visitTable_source(VerdictSQLParser.Table_sourceContext ctx) {
    return visitTable_source_item_joined(ctx.table_source_item_joined());
  }

  @Override
  public AbstractRelation visitTable_source_item_joined(
      VerdictSQLParser.Table_source_item_joinedContext ctx) {
    AbstractRelation r = this.visit(ctx.table_source_item());
    if (ctx.join_part().isEmpty()) {
      return r;
    }
    JoinTable jr =
        JoinTable.createBase(
            r, new ArrayList<JoinTable.JoinType>(), new ArrayList<UnnamedColumn>());
    // join error location: r2 is null
    for (VerdictSQLParser.Join_partContext j : ctx.join_part()) {
      AbstractRelation r2 = visit(j);
      jr.addJoinTable(r2, joinType, joinCond);
    }
    return jr;
  }

  @Override
  public AbstractRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
    if (ctx.INNER() != null) {
      if (ctx.search_condition() == null) {
        throw new RuntimeException("The join condition for a inner join does not exist.");
      }
      
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.inner;
      joinCond = cond;
      return r;
    } else if (ctx.LEFT() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.leftouter;
      joinCond = cond;
      return r;
    } else if (ctx.RIGHT() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.rightouter;
      joinCond = cond;
      return r;
    } else if (ctx.CROSS() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      joinType = JoinTable.JoinType.cross;
      joinCond = null;
      return r;
    } else {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      if (ctx.search_condition() != null) {
        UnnamedColumn cond = g.visit(ctx.search_condition());
        joinType = JoinTable.JoinType.inner;
        joinCond = cond;
      } else {
        joinType = JoinTable.JoinType.cross;
        joinCond = null;
      }
      return r;
    }
  }

  private String stripQuote(String expr) {
    return expr.replace("\"", "").replace("`", "");
  }

  @Override
  public AbstractRelation visitHinted_table_name_item(
      VerdictSQLParser.Hinted_table_name_itemContext ctx) {
    AbstractRelation r = null;
    if (ctx.as_table_alias() != null) {
      if (ctx.table_name_with_hint().table_name().schema != null) {
        r =
            new BaseTable(
                stripQuote(ctx.table_name_with_hint().table_name().schema.getText()),
                stripQuote(ctx.table_name_with_hint().table_name().table.getText()),
                stripQuote(ctx.as_table_alias().table_alias().getText()));
      } else {
        r =
            BaseTable.getBaseTableWithoutSchema(
                stripQuote(ctx.table_name_with_hint().table_name().table.getText()),
                stripQuote(ctx.as_table_alias().table_alias().getText()));
      }
    } else {
      if (ctx.table_name_with_hint().table_name().schema != null) {
        r =
            new BaseTable(
                stripQuote(ctx.table_name_with_hint().table_name().schema.getText()),
                stripQuote(ctx.table_name_with_hint().table_name().table.getText()));
      } else {
        r = new BaseTable(stripQuote(ctx.table_name_with_hint().table_name().table.getText()));
      }
    }
    return r;
  }

  @Override
  public AbstractRelation visitDerived_table_source_item(
      VerdictSQLParser.Derived_table_source_itemContext ctx) {
    RelationGen gen = new RelationGen();
    SelectQuery r = (SelectQuery) gen.visit(ctx.derived_table().subquery().select_statement());
    if (ctx.as_table_alias() != null) {
      r.setAliasName(ctx.as_table_alias().table_alias().getText());
    }
    if (ctx.column_alias_list() != null) {
      for (int i = 0; i < ctx.column_alias_list().column_alias().size(); i++) {
        r.getSelectList()
            .set(
                i,
                new AliasedColumn(
                    (UnnamedColumn) r.getSelectList().get(i),
                    ctx.column_alias_list().column_alias(i).getText()));
      }
    }
    return r;
  }

  @Override
  public AbstractRelation visitTable_name(VerdictSQLParser.Table_nameContext ctx) {
    String schemaName = (ctx.schema == null) ? "" : ctx.schema.getText();
    String tableName = (ctx.table == null) ? "" : ctx.table.getText();

    return new BaseTable(schemaName, tableName);
  }
}
