package org.example;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import static org.example.CalciteUtils.getOptimizedQueryRelNode;

public class Main {

    static final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    static FrameworkConfig frameworkConfig;

    static {
        double scaleFactor = 0.01;
        CalciteSchema tpcds = rootSchema.add("tpc_ds", new TpcdsSchema(scaleFactor));

        frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(tpcds.plus())
                .parserConfig(SqlParser.Config.DEFAULT.withCaseSensitive(false))
                .build();
    }

    public static void main(String[] args) {
        boolean debugPlan = true;
        RelNode exampleQuery1Rel = getOptimizedQueryRelNode(exampleQuery1, frameworkConfig, debugPlan);
        System.out.println(RelOptUtil.toString(exampleQuery1Rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }


    static String exampleQuery1 = """
            SELECT
                s_store_name,
                i_item_desc,
                revenue
            FROM
                store,
                item,
                (
                    SELECT
                        ss_store_sk,
                        AVG(revenue) AS ave
                    FROM (
                        SELECT
                            ss_store_sk,
                            ss_item_sk,
                            sum(ss_sales_price) AS revenue
                        FROM
                            store_sales,
                            date_dim
                        WHERE
                            ss_sold_date_sk = d_date_sk
                            AND d_month_seq BETWEEN 1212 AND 1247
                        GROUP BY
                            ss_store_sk,
                            ss_item_sk) sa
                    GROUP BY
                        ss_store_sk) sb,
                (
                    SELECT
                        ss_store_sk,
                        ss_item_sk,
                        sum(ss_sales_price) AS revenue
                    FROM
                        store_sales,
                        date_dim
                    WHERE
                        ss_sold_date_sk = d_date_sk
                        AND d_month_seq BETWEEN 1212 AND 1247
                    GROUP BY
                        ss_store_sk,
                        ss_item_sk) sc
            WHERE
                sb.ss_store_sk = sc.ss_store_sk
                AND sc.revenue <= 0.1 * sb.ave
                AND s_store_sk = sc.ss_store_sk
                AND i_item_sk = sc.ss_item_sk
            ORDER BY
                s_store_name,
                i_item_desc
            LIMIT 100
            """;

}
