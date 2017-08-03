/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.test.api.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.batch.history.HistoricBatch;
import org.camunda.bpm.engine.history.HistoricIncident;
import org.camunda.bpm.engine.history.HistoricJobLog;
import org.camunda.bpm.engine.impl.batch.BatchEntity;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricIncidentEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogEventEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.management.Metrics;
import org.camunda.bpm.engine.migration.MigrationPlan;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.camunda.bpm.engine.test.RequiredHistoryLevel;
import org.camunda.bpm.engine.test.api.runtime.migration.MigrationTestRule;
import org.camunda.bpm.engine.test.api.runtime.migration.batch.BatchMigrationHelper;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

@RequiredHistoryLevel(ProcessEngineConfiguration.HISTORY_FULL)
public class HistoryCleanupHistoricBatchTest {

  public ProcessEngineRule engineRule = new ProcessEngineRule(true);
  public ProcessEngineTestRule testRule = new ProcessEngineTestRule(engineRule);
  protected MigrationTestRule migrationRule = new MigrationTestRule(engineRule);
  protected BatchMigrationHelper helper = new BatchMigrationHelper(engineRule, migrationRule);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(engineRule).around(testRule).around(migrationRule);

  protected RuntimeService runtimeService;
  protected HistoryService historyService;
  protected ManagementService managementService;
  protected ProcessEngineConfigurationImpl processEngineConfiguration;

  @Before
  public void init() {
    runtimeService = engineRule.getRuntimeService();
    historyService = engineRule.getHistoryService();
    managementService = engineRule.getManagementService();
    processEngineConfiguration = engineRule.getProcessEngineConfiguration();
  }

  @After
  public void clearDatabase() {
    helper.removeAllRunningAndHistoricBatches();

    processEngineConfiguration.getCommandExecutorTxRequired().execute(new Command<Void>() {
      public Void execute(CommandContext commandContext) {

        List<Job> jobs = managementService.createJobQuery().list();
        if (jobs.size() > 0) {
          assertEquals(1, jobs.size());
          String jobId = jobs.get(0).getId();
          commandContext.getJobManager().deleteJob((JobEntity) jobs.get(0));
          commandContext.getHistoricJobLogManager().deleteHistoricJobLogByJobId(jobId);
        }

        List<HistoricIncident> historicIncidents = historyService.createHistoricIncidentQuery().list();
        for (HistoricIncident historicIncident : historicIncidents) {
          commandContext.getDbEntityManager().delete((HistoricIncidentEntity) historicIncident);
        }

        commandContext.getMeterLogManager().deleteAll();

        return null;
      }
    });
  }

  @Test
  public void testCleanupHistoricBatch() {
    Date startDate = ClockUtil.getCurrentTime();
    int daysInThePast = -11;
    ClockUtil.setCurrentTime(DateUtils.addDays(startDate, daysInThePast));

    // given
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(5);
    prepareHistoricBatches(3, daysInThePast);

    // when
    List<HistoricBatch> historicList = historyService.createHistoricBatchQuery().list();
    assertEquals(3, historicList.size());
    String jobId = historyService.cleanUpHistoryAsync(true).getId();

    managementService.executeJob(jobId);

    // then
    assertEquals(0, historyService.createHistoricBatchQuery().count());
  }

  @Test
  public void testCleanupHistoricJobLog() {
    Date startDate = ClockUtil.getCurrentTime();
    int daysInThePast = -11;
    ClockUtil.setCurrentTime(DateUtils.addDays(startDate, daysInThePast));

    // given
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(5);
    prepareHistoricBatches(1, daysInThePast);
    HistoricBatch batch = historyService.createHistoricBatchQuery().singleResult();
    String batchId = batch.getId();

    // when
    String jobId = historyService.cleanUpHistoryAsync(true).getId();

    managementService.executeJob(jobId);

    // then
    assertEquals(0, historyService.createHistoricBatchQuery().count());
    assertEquals(0, historyService.createHistoricJobLogQuery().jobDefinitionConfiguration(batchId).count());
  }

//  @Test
//  public void testCleanupHistoricIncident() {
//    // given
//    processEngineConfiguration.setHistoricBatchHistoryTimeToLive(5);
//
//    ClockUtil.setCurrentTime(DateUtils.addDays(new Date(), -11));
//
//    Batch batch = createMigrationBatch();
//    createIncident();
//    executeBatch(batch);
//
//    // when
//    String jobId = historyService.cleanUpHistoryAsync(true).getId();
//
//    managementService.executeJob(jobId);
//
//    // then
//    assertEquals(0, historyService.createHistoricIncidentQuery().count());
//  }

  @Test
  public void testCleanupHistoricIncident() {
    // given
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(5);

    ClockUtil.setCurrentTime(DateUtils.addDays(new Date(), -11));

    BatchEntity batch = (BatchEntity) createMigrationBatch();

    helper.executeSeedJob(batch);

    List<Job> list = managementService.createJobQuery().list();
    for (Job job : list) {
      if (((JobEntity) job).getJobHandlerType().equals("instance-migration")) {
        managementService.setJobRetries(job.getId(), 1);
      }
    }
    helper.executeJobs(batch);

    ClockUtil.setCurrentTime(DateUtils.addDays(new Date(), -10));
    helper.executeMonitorJob(batch);

    ClockUtil.setCurrentTime(new Date());

    // when TODO - ne moga da go iztriq
    String jobId = historyService.cleanUpHistoryAsync(true).getId();

    managementService.executeJob(jobId);

//    HistoricIncident historicIncident = historyService.createHistoricIncidentQuery().singleResult();
//    assertNull(historicIncident);

    List<String> byteArrayIds = findExceptionByteArrayIds();

    verifyByteArraysWereRemoved(byteArrayIds.toArray(new String[] {}));
    assertEquals(0, historyService.createHistoricBatchQuery().count());
    assertEquals(0, historyService.createHistoricIncidentQuery().count());
    assertEquals(0, historyService.createHistoricJobLogQuery().count());
  }

  @Test
  public void testHistoryCleanupBatchMetrics() {
    // given
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(5);

    int daysInThePast = -11;
    int batchesCount = 5;
    prepareHistoricBatches(batchesCount, daysInThePast);

    // when
    String jobId = historyService.cleanUpHistoryAsync(true).getId();

    managementService.executeJob(jobId);

    // then
    final long removedBatches = managementService.createMetricsQuery().name(Metrics.HISTORY_CLEANUP_REMOVED_BATCH_INSTANCES).sum();

    assertEquals(batchesCount, removedBatches);
  }

  @Test
  public void testConfiguration() {
    int days = 5;
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(days);
    processEngineConfiguration.initHistoryCleanup();
    assertEquals(days, processEngineConfiguration.getBatchOperationHistoryTimeToLive().intValue());
  }

  @Test
  public void testConfigurationFailure() {
    processEngineConfiguration.setBatchOperationHistoryTimeToLive(-1);

    thrown.expect(ProcessEngineException.class);
    thrown.expectMessage("batchOperationHistoryTimeToLive");

    processEngineConfiguration.initHistoryCleanup();
  }

  private BpmnModelInstance createModelInstance() {
    BpmnModelInstance instance = Bpmn.createExecutableProcess("process")
        .startEvent()
        .userTask("userTask")
        .endEvent()
        .done();
    return instance;
  }

  private Batch createMigrationBatch() {
    BpmnModelInstance instance = createModelInstance();

    ProcessDefinition sourceProcessDefinition = migrationRule.deployAndGetDefinition(instance);
    ProcessDefinition targetProcessDefinition = migrationRule.deployAndGetDefinition(instance);

    MigrationPlan migrationPlan = runtimeService
        .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
        .mapEqualActivities()
        .build();

    ProcessInstance processInstance = runtimeService.startProcessInstanceById(sourceProcessDefinition.getId());

    Batch batch = runtimeService.newMigration(migrationPlan).processInstanceIds(Arrays.asList(processInstance.getId(), "ser")).executeAsync();
    return batch;
  }

//  private void createIncident() {
//    Job job = managementService.createJobQuery().singleResult();
//    managementService.setJobRetries(job.getId(), 0);
//
//    Incident incident = runtimeService.createIncidentQuery().singleResult();
//    assertNotNull(incident);
//  }

//  private void executeBatch(Batch batch) {
//    helper.executeSeedJob(batch);
//    helper.executeJobs(batch);
//
//    ClockUtil.setCurrentTime(DateUtils.addDays(new Date(), -10));
//    helper.executeMonitorJob(batch);
//
//    ClockUtil.setCurrentTime(new Date());
//
//    HistoricIncident historicIncident = historyService.createHistoricIncidentQuery().singleResult();
//    assertNotNull(historicIncident);
//  }

  private void prepareHistoricBatches(int batchesCount, int daysInThePast) {
    Date startDate = ClockUtil.getCurrentTime();
    ClockUtil.setCurrentTime(DateUtils.addDays(startDate, daysInThePast));

    List<Batch> list = new ArrayList<Batch>();
    for (int i = 0; i < batchesCount; i++) {
      list.add(helper.migrateProcessInstancesAsync(1));
    }

    for (Batch batch : list) {
      helper.executeSeedJob(batch);
      helper.executeJobs(batch);

      ClockUtil.setCurrentTime(DateUtils.addDays(startDate, ++daysInThePast));
      helper.executeMonitorJob(batch);
    }

    ClockUtil.setCurrentTime(new Date());
  }

  private void verifyByteArraysWereRemoved(final String... errorDetailsByteArrayIds) {
    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Void>() {
      public Void execute(CommandContext commandContext) {
        for (String errorDetailsByteArrayId : errorDetailsByteArrayIds) {
          assertNull(commandContext.getDbEntityManager().selectOne("selectByteArray", errorDetailsByteArrayId));
        }
        return null;
      }
    });
  }

  private List<String> findExceptionByteArrayIds() {
    List<String> exceptionByteArrayIds = new ArrayList<String>();
    List<HistoricJobLog> historicJobLogs = historyService.createHistoricJobLogQuery().list();
    for (HistoricJobLog historicJobLog : historicJobLogs) {
      HistoricJobLogEventEntity historicJobLogEventEntity = (HistoricJobLogEventEntity) historicJobLog;
      if (historicJobLogEventEntity.getExceptionByteArrayId() != null) {
        exceptionByteArrayIds.add(historicJobLogEventEntity.getExceptionByteArrayId());
      }
    }
    return exceptionByteArrayIds;
  }

}
