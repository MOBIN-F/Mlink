package com.mobin

import org.junit.{After, Before, Test}

class FlinkSQLDemo {

  var mlinkClient: FlinkClient = _

  var sql: String = _

  @Before
  def setup(): Unit = {
    mlinkClient = new FlinkClient()
  }

  @After
  def execute(): Unit = {
    val result = mlinkClient.executeInitialization(sql)
    if (result != null) {
      result.getJobClient.get().getJobExecutionResult.get()
    }
  }

  @Test
  def testDatagenToPrint(): Unit = {
    sql =
      """
        |SET 'execution.checkpointing.interval' = '1min';
        |SET 'execution.checkpointing.min-pause' = '10s';
        |
        |CREATE TABLE source_datagen (
        |    user_id INT,
        |    cost DOUBLE,
        |    ts AS localtimestamp,
        |    procTime AS PROCTIME()
        |) WITH (
        |      'connector' = 'datagen',
        |      'rows-per-second'='1',
        |      'fields.user_id.kind'='random',
        |      'fields.user_id.min'='1',
        |      'fields.user_id.max'='10',
        |      'fields.cost.kind'='random',
        |      'fields.cost.min'='1',
        |      'fields.cost.max'='100'
        |      );
        |
        |CREATE TABLE print (
        |    user_id           INT,
        |    cost              DOUBLE
        |) WITH (
        |      'connector' = 'print'
        |      );
        |
        |INSERT INTO print
        |SELECT
        |    user_id,
        |    cost
        |FROM source_datagen;
        |
        |""".stripMargin
  }
}