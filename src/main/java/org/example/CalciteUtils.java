package org.example;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.ValidationException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CalciteUtils {
    private CalciteUtils() {
    }

    static RelNode convertSqlToRel(String sql, FrameworkConfig frameworkConfig) {
        try {
            Planner planner = Frameworks.getPlanner(frameworkConfig);
            SqlNode sqlNode = planner.parse(sql);
            sqlNode = planner.validate(sqlNode);
            return planner.rel(sqlNode).project();
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            throw new RuntimeException(e);
        }
    }

    static String relNodeToSql(RelNode relNode) {
        return new RelToSqlConverter(CalciteSqlDialect.DEFAULT)
                .visitRoot(relNode)
                .asStatement()
                .toSqlString(CalciteSqlDialect.DEFAULT)
                .toString();
    }

    static RelNode getOptimizedQueryRelNode(String sql, FrameworkConfig frameworkConfig, boolean explain) {
        if (explain) {
            System.out.println("sql = " + sql);
        }

        RelNode relRoot = convertSqlToRel(sql, frameworkConfig);
        if (explain) {
            System.out.println(RelOptUtil.dumpPlan(
                    "-- Logical Plan", relRoot, SqlExplainFormat.TEXT,
                    SqlExplainLevel.DIGEST_ATTRIBUTES
            ));
        }

        RelOptCluster cluster = relRoot.getCluster();
        RelOptPlanner optPlanner = cluster.getPlanner();
        RelNode newRoot = optPlanner.changeTraits(relRoot, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        if (explain) {
            System.out.println(RelOptUtil.dumpPlan(
                    "-- Mid Plan", newRoot, SqlExplainFormat.TEXT,
                    SqlExplainLevel.DIGEST_ATTRIBUTES
            ));
        }

        optPlanner.setRoot(newRoot);
        RelNode bestExp = optPlanner.findBestExp();
        if (explain) {
            System.out.println(RelOptUtil.dumpPlan(
                    "-- Best Plan", bestExp, SqlExplainFormat.TEXT,
                    SqlExplainLevel.DIGEST_ATTRIBUTES
            ));
        }

        return bestExp;
    }

    static ResultSet executeRelNode(RelNode bestExp, CalciteConnection connection) {
        try {
            RelRunner relRunner = connection.unwrap(RelRunner.class);
            return relRunner.prepareStatement(bestExp).executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
