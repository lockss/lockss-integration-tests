/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.state;

import java.io.*;
import java.util.*;
import java.util.stream.*;
import javax.jms.*;
import org.junit.*;
import org.apache.commons.lang3.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.config.*;
import org.lockss.log.*;
import org.lockss.jms.*;
import org.lockss.plugin.*;
import org.lockss.poller.v3.V3Poller;
import org.lockss.poller.v3.V3Poller.PollVariant;
import org.lockss.protocol.*;
import static org.lockss.protocol.AgreementType.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.time.TimerUtil;
import org.lockss.util.time.TimeBase;

/** 
 * Integration test for StateManagers.  Starts several clients performing
 * state updates and assertions.  Requires ConfigService to be running.
 *
 * To run all subtests:
 * mvn -o test -Dtest=FuncStateManager* -Dlogdir=target/surefire-reports -DtestForkCount=1 -Dloglevel=debug
 *
 * To run one subtest:
 * mvn -o test -Dtest=FuncStateManager*AuState1 -Dlogdir=target/surefire-reports -DtestForkCount=1 -Dloglevel=debug
 *
 * testForkCount is currently required because JMS comm uses a fixed topic
 * name, making for a global comm channel.  Simultaneous tests result in
 * messages co-mingling.
*/
public abstract class FuncStateManager extends StateTestCase {
  static L4JLogger log = L4JLogger.getLogger();

  // XXX Need to parameterize
  static final String CFG_SVC_URL = "http://localhost:24620";
  static final String JMS_BROKER_URL = "tcp://localhost:61616";


  static final String JMS_TOPIC = "FuncStateManager";

  // System properties used to pass info to subprocesses
  static final String SYSPROP_TEST_INSTANCE = "testinst";
  static final String SYSPROP_NUM_INSTANCES = "numinst";
  static final String SYSPROP_LOG_DIR = "logdir";

  String OUTER_CLASS_SHORT_NAME =
    StringUtil.truncateAtAny(StringUtil.shortName(this.getClass()), "$");


  MockPlugin mplug;
  Producer prod;
  Consumer cons;

  File logdir;
  int numinst;				// number of client instances
  int testinst;				// this client instance number
  Boolean keeptempfiles;
  String loglevel;
  int stepTimeout = (int)Constants.MINUTE;
  String gauid1;
  String gauid2;


  @Before
  public void setUp() throws Exception {

    // Record params to pass down to subprocesses
    keeptempfiles = Boolean.getBoolean("keeptempfiles");
    if (keeptempfiles == null) {
      keeptempfiles = false;
    }
    loglevel = System.getProperty("loglevel", "");

    System.setProperty(ConfigManager.SYSPROP_REST_CONFIG_SERVICE_URL,
		       CFG_SVC_URL);
    super.setUp();

    daemon = getMockLockssDaemon();
    pluginMgr = daemon.getPluginManager();
//     pluginMgr.startService();

    mplug = new MockPlugin(daemon);

    try {
      cons = Consumer.createTopicConsumer(null, JMS_TOPIC,
					  new MyMessageListener("Test Listener"));
      prod = Producer.createTopicProducer(null, JMS_TOPIC);
    } catch (JMSException e) {
      fail("Couldn't create JMS producer or consumer.  Likely cause is that ConfigService is not running; it is required for this test.");
    }
  }

  @After
  public void tearDown() throws Exception {
    prod.closeConnection();
    cons.closeConnection();
    daemon.stopManagers();
    super.tearDown();
  }

  /** Additional setup when running as master.
   *
   * Setup a log dir to which to redirect client output.  Make relative to
   * base log dir if that was passed in
   */
  void setUpMaster() throws IOException {
    String dir = System.getProperty(SYSPROP_LOG_DIR);
    if (!StringUtil.isNullString(dir)) {
      logdir = new File(dir, "clientlogs");
      FileUtil.ensureDirExists(logdir);
    } else {
      logdir = getTempDir("clientlogs");
    }
    log.debug2("logdir: {}", logdir.getAbsoluteFile());
  }

  /** Additional setup when running as client.
   *
   * For each action, create a client-gathering array of the appropriate
   * size.
   */
  void setupClients(int numclients) {
    for (Action act : ListUtil.list(Action.ClientAssert, Action.ClientReady)) {
      clientMaps.put(act, new Map[numclients]);
    }
  }    

  @Override
  protected void startManagers() {
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_BROKER_URI, JMS_BROKER_URL);
    daemon.startManagers(JMSManager.class);
    super.startManagers();
  }

  @Override
  protected StateManager makeStateManager() {
    return new ClientStateManager();
  }

  //////////////////////
  //  Utilities
  //////////////////////

  /** Create a unique AUID */
  String makeAuid(String base) {
    return base + "-" +
      org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(8);
  }

  boolean isMaster() {
    return StringUtil.isNullString(System.getProperty(SYSPROP_TEST_INSTANCE));
  }

  /** Create a name unique to the test subclass and client instance.  Use
   * for temp and log files */
  String uniqueName(String base, int ix) {
    String cls = StringUtil.shortName(this.getClass());
    int pos = cls.indexOf("$");
    String namepart = (pos > 0) ? cls.substring(pos + 1) : cls;
    return base + "-" + namepart + "-" + ix;
  }

  //////////////////////
  // Exec client harness
  //////////////////////

  /** Start a process to run a client */
  Process execClientTest(int ix) throws IOException {
    int exitValue = -1;
    String[] command =
      new String[]{"mvn", "-o", /*"-q",*/ "test",
		   "-Dtest=" + OUTER_CLASS_SHORT_NAME + "," + this.getClass().getName(),
		   // each surefire invocation need its own temp dir
		   "-DtempDir=" + uniqueName("surefire", ix),
		   // this one doesn't seem to work
		   "-DreportsDirectory=" + uniqueName("surefire-reports", ix),
		   
		   // antrun has problems w/ multiple mvn invocations
		   "-DskipEtags=true",
		   "-DskipClasspathFiles=true",
		   "-DskipBuildInfo=true",
		   "-DskipDocker=true",

		   "-Doutputtofile=false",
		   "-Dkeeptempfiles=" + keeptempfiles,
		   "-Dloglevel=" + loglevel,
		   "-D" + SYSPROP_TEST_INSTANCE + "=" + ix,
		   "-D" + SYSPROP_NUM_INSTANCES + "=" + numinst,
    };
    try {
      ProcessBuilder pb = new ProcessBuilder(command)
	.redirectErrorStream(true)
	.redirectOutput(new File(logdir, uniqueName("client", ix)));
      log.debug("Command: {}", pb.command());
      Process proc = pb.start();
      return proc;
    } catch (IOException e) {
      log.warn("Exec failed", e);
      return null;
    }
  }

  /** Start all the clients */
  Process[] execClientTests(int cnt) throws IOException {
    numinst = cnt;
    setupClients(numinst);

    Process[] procs = new Process[cnt];
    for (int ix = 1; ix <= cnt; ix++) {
      procs[ix - 1] = execClientTest(ix);
    }
    return procs;
  }

  /** Wait for a process to finish, return its exit status */
  int waitFor(Process proc) {
    try {
      int exitValue = proc.waitFor();
      log.debug2("exitcode: {}", exitValue);
      return exitValue;
    } catch (InterruptedException e) {
      log.error("interrupted", e);
      return -1000;
    }
  }

  /** Wait for all spawned tests to complete, return a list of their exit
   * codes */
  List<Integer> waitForAll(Process[] procs) {
    List<Integer> res = new ArrayList<>();
    for (Process proc : procs) {
      res.add(waitFor(proc));
    }
    return res;
  }

  /** Return list of indices of clients that failed */
  List<Integer> failingIndices(List<Integer> rets) {
    return IntStream.range(0, rets.size())
      .filter(i -> rets.get(i) != 0)
      .mapToObj(i -> i + 1)
      .collect(Collectors.toList());
  }

  /** Wait for all clients to complete, assert that they all succeeded */
  void waitForAllToSucceed(Process[] procs) {
    List<Integer> fails = failingIndices(waitForAll(procs));
    if (!fails.isEmpty()) {
      fail("Clients failed: " + fails);
    }
  }

  ///////////////////////////////
  // Command queue
  ///////////////////////////////

  FifoQueue cmdQueue = new FifoQueue();

  void enqueue(Action act, Map map) {
    Map copy = new HashMap(map);
    map.put(JMS_MAP_ACTION, act);
    cmdQueue.put(map);
  }

  Map waitForCommand(Action act) throws Exception {
    Map map = (Map)cmdQueue.get(Deadline.in(stepTimeout));
    if (map == null) {
      fail("Timed out awaiting expected command: " + act);
    }
    assertEquals(act, msgAction(map));
    return map;
  }


  ///////////////////////////////
  // Inter-client message harness
  ///////////////////////////////

  /** Send a command to all clients and master */
  void send(Action act) {
    send(act, new HashMap());
  }

  /** Send a command with data to all clients and master */
  void send(Action act, Map map) {
    map.put(JMS_MAP_ACTION, act.toString());
    map.put(JMS_MAP_INST, testinst);
    try {
      prod.sendMap(map);
    } catch (JMSException e) {
      fail("Couldn't send JMS message", e);
    }
  }

  /** Return action from msg map as Action */
  Action msgAction(Map map) {
    Object o = map.get(JMS_MAP_ACTION);
    if (o instanceof Action) {
      return (Action)o;
    } else {
      return getAction((String)o);
    }
  }

  /** Return client instance number from map */
  int msgInst(Map map) {
    return (Integer)map.get(JMS_MAP_INST);
  }


  static final String JMS_MAP_ACTION = "action";
  static final String JMS_MAP_INST = "inst";
  static final String JMS_MAP_AUIDS = "auids";

  enum Action {Unknown,
	       ClientReady, ClientReadyGo,
	       ClientAssert, ClientAssertGo
  };

  /** Turn an action name into an Action */
  private Action getAction(String act) {
    try {
      return Enum.valueOf(Action.class, act);
    } catch (IllegalArgumentException e) {
      log.error("Not an enum of type Action: {}", act);
      return Action.Unknown;
    }
  }

  /** Accumulator, per action type, of message from each client. */
  Map<Action,Map[]> clientMaps = new HashMap<>();

  /** Record that an action message has been received from the client;
   * return true iff that message has now been received from all clients.
   */
  boolean collect(Action act, Map map) {
    int i = (int)map.get(JMS_MAP_INST);
    Map[] cliMap = clientMaps.get(act);
    if (cliMap[i-1] != null) {
      log.warn("Already received expected {} msg from client {}", act, i);
    }
    cliMap[i-1] = map;
    return Arrays.stream(cliMap).allMatch(x -> x != null);
  }    

  /** Handle incoming message from client or master */
  protected void receiveMessage(Map map) {
    log.debug2("Received: {}" , map);
    if (isMaster()) {
      masterReceiveMessage(map);
    } else {
      clientReceiveMessage(map);
    }
  }
  
  /** Master processes ClientReady, sends ClientReadyGo once ClientReady
   * has been received from all clients.  ClientReadyGo message is also
   * used to convey the randomly-generated AUIDs to all clients.
   */
  protected void masterReceiveMessage(Map map) {
    log.debug("Received {} from client {}", msgAction(map), msgInst(map));
    Action act = getAction((String)map.get(JMS_MAP_ACTION));
    switch (act) {
    case ClientReady:
      if (collect(act, map)) {
	send(Action.ClientReadyGo, MapUtil.map(JMS_MAP_AUIDS,
					       ListUtil.list(makeAuid(AUID1),
							     makeAuid(AUID2))));
      }
      break;
    }
  }

  /** Client processes:
   *
   * ClientReadyGo - enqueue ClientReadyGo command to start client actions
   *
   * ClientAssert - once received from all clients, enqueue ClientAssertGo
   * to start client asserts
   */
  protected void clientReceiveMessage(Map map) {
    Action act = getAction((String)map.get(JMS_MAP_ACTION));
    switch (act) {
    case ClientReadyGo:
      enqueue(act, map);
      break;
    case ClientAssert:
      if (collect(act, map)) {
	enqueue(Action.ClientAssertGo, map);
      }
      break;
    }
  }

  /** Listener for test client/master messages */
  private class MyMessageListener
    extends Consumer.SubscriptionListener {

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      try {
        Object msgObject =  Consumer.convertMessage(message);
	if (msgObject instanceof Map) {
	  receiveMessage((Map)msgObject);
	} else {
	  log.error("Unknown notification type: " + msgObject);
	}
      } catch (JMSException e) {
	log.warn("foo", e);
      }
    }
  }


  //////////////////////
  // Test harness
  //////////////////////

  /** This (the sole junit test method) determines whether to act as the
   * master or a client, performs appropriate setup and dispatch
   */
  @Test
  public void testDispatch() throws Exception {
    if (isMaster()) {
      setUpMaster();
      runMaster();
    } else {
      Integer n = Integer.getInteger(SYSPROP_TEST_INSTANCE);
      testinst = n;
      Integer cnt = Integer.getInteger(SYSPROP_NUM_INSTANCES);
      if (cnt == null) {
	fail(SYSPROP_NUM_INSTANCES + " not passed to client");
      }
      numinst = cnt;
      setupClients(numinst);
      runClient();
    }
  }

  /** Subclass must implement */
  abstract void runMaster() throws Exception;
  /** Subclass must implement */
  abstract void runClient() throws Exception;


  //////////////////////
  // Tests
  //////////////////////


  /** Verify that test harness works */
  public static class Succeed extends FuncStateManager {

    void runMaster() throws IOException {
      Process[] procs = execClientTests(2);
      log.debug("waiting for success");
      waitForAllToSucceed(procs);
    }

    void runClient() {
    }
  }

  /** Verify that harness detects client failure */
  public static class Fail extends FuncStateManager {
    void runMaster() throws IOException {
      Process[] procs = execClientTests(2);
      log.debug("waiting");
      assertEquals(ListUtil.list(1, 0), waitForAll(procs));
    }

    void runClient() {
      if (testinst == 1) fail("Failed: " + testinst);
    }
  }

  /** Harness to run one cycle of update/assert */
  public abstract static class OneCycle extends FuncStateManager {
    void runMaster() throws IOException {
      Process[] procs = execClientTests(3);
      waitForAllToSucceed(procs);
    }

    abstract void doUpdates(Map map) throws Exception;
    abstract void doAsserts(Map map) throws Exception;

    void runClient() throws Exception {
      send(Action.ClientReady);
      Map map = waitForCommand(Action.ClientReadyGo);
      setupAus(map);
      doUpdates(map);
      send(Action.ClientAssert);
      map = waitForCommand(Action.ClientAssertGo);
      doAsserts(map);
    }
  }

  /** Create new AUs, ignoring those set up by StateTestCase.  In order to
   * get repeatable results we need to use new AUs each time, for which
   * ConfigSvc doesn't already have data.
   */
  void setupAus(Map map) {
    List<String> auids = (List<String>)map.get(JMS_MAP_AUIDS);
    MockPlugin plugin = new MockPlugin(daemon);
    gauid1 = auids.get(0);
    mau1 = new MockArchivalUnit(plugin, gauid1);
    gauid2 = auids.get(1);
    mau2 = new MockArchivalUnit(plugin, gauid2);
  }

  public static class AuState1 extends OneCycle {
    AuState aus1, aus2;
    AuAgreements aua1, aua2;

    void doUpdates(Map map) {
      aus1 = stateMgr.getAuState(mau1);
      aus2 = stateMgr.getAuState(mau2);
      aua1 = stateMgr.getAuAgreements(gauid1);
      aua2 = stateMgr.getAuAgreements(gauid2);
      assertNotSame(aus1, aus2);
      assertSame(aus1, stateMgr.getAuState(mau1));
      assertSame(aus2, stateMgr.getAuState(mau2));
      assertNotSame(aua1, aua2);
      assertSame(aua1, stateMgr.getAuAgreements(gauid1));
      assertSame(aua2, stateMgr.getAuAgreements(gauid2));
      
      switch (testinst) {
      case 1: updateAus1(); updateAua1(); break;
      case 2: updateAus2(); updateAua2(); break;
      case 3: updateAus3(); break;
      default: throw new IllegalArgumentException("Unknown client instance: "
						  + testinst);
      }
    }

    void doAsserts(Map map) throws Exception {
      // XXX need a way to wait until all changes have propagated
      TimerUtil.sleep(1000);
      assertAus1();
      assertAus2();
      assertAus3();
      assertAua1();
      assertAua2();
    }

    void updateAus1() {
      assertEquals(-1, aus1.getLastCrawlTime());
      TimeBase.setSimulated(1111);
      aus1.newCrawlStarted();
      TimeBase.step(2);
      aus1.newCrawlFinished(Crawler.STATUS_SUCCESSFUL, "result", -1);
      assertAus1();
    }

    void assertAus1() {
      assertEquals(1111, aus1.getLastCrawlAttempt());
      assertEquals("result", aus1.getLastCrawlResultMsg());
      assertEquals(Crawler.STATUS_SUCCESSFUL, aus1.getLastCrawlResult());
      assertSame(aus1, stateMgr.getAuState(mau1));
    }

    void updateAus2() {
      assertEquals(-1, aus1.getLastPollStart());
      TimeBase.setSimulated(2222);
      aus1.pollStarted();
      TimeBase.step(20);
      aus1.pollFinished(V3Poller.POLLER_STATUS_COMPLETE,
			V3Poller.PollVariant.PoR);
      assertAus2();
    }

    void assertAus2() {
      assertEquals(2222, aus1.getLastPollStart());
      assertEquals(V3Poller.POLLER_STATUS_COMPLETE, aus1.getLastPollResult());
      assertSame(aus1, stateMgr.getAuState(mau1));
    }


    static final String CDN_STEM_1 = "http://foo.bar/";
    static final String CDN_STEM_2 = "http://bar.foo/";
    static final String CDN_STEM_3 = "http://rab.oof/";
    static final List<String> CDN_LIST = ListUtil.list(CDN_STEM_1, CDN_STEM_2);

    void updateAus3() {
      aus1.setLastMetadataIndex(45451);
      aus2.setLastMetadataIndex(45452);
      aus1.setCdnStems(CDN_LIST);
      aus1.addCdnStem(CDN_STEM_3);
    }

    void assertAus3() {
      assertEquals(ListUtil.append(CDN_LIST, ListUtil.list(CDN_STEM_3)),
		   aus1.getCdnStems());
      assertEquals(45451, aus1.getLastMetadataIndex());
      assertEquals(45452, aus2.getLastMetadataIndex());
    }

    void updateAua1() {
      assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid1, POR));

      aua1.signalPartialAgreement(pid0, POR, .9f, 800);
      aua1.signalPartialAgreement(pid0, POP, .7f, 800);

      aua1.signalPartialAgreement(pid1, POR, .25f, 900);
      aua1.signalPartialAgreement(pid1, POP, .50f, 900);

      aua2.signalPartialAgreement(pid0, POR, .10f, 910);
      aua2.signalPartialAgreement(pid0, POP, .20f, 910);
      storeAuAgreements(aua1, pid0, pid1);
      storeAuAgreements(aua2, pid0);
      assertAua1();
    }

    void assertAua1() {
      assertAgreeTime(.9f, 800, aua1.findPeerAgreement(pid0, POR));
      assertAgreeTime(.7f, 800, aua1.findPeerAgreement(pid0, POP));
      assertAgreeTime(.25f, 900, aua1.findPeerAgreement(pid1, POR));
      assertAgreeTime(.50f, 900, aua1.findPeerAgreement(pid1, POP));
      assertAgreeTime(.10f, 910, aua2.findPeerAgreement(pid0, POR));
      assertAgreeTime(.20f, 910, aua2.findPeerAgreement(pid0, POP));

      assertSame(aua1, stateMgr.getAuAgreements(gauid1));
    }

    void updateAua2() {
      assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid2, POR));

      aua1.signalPartialAgreement(pid2, POR, .95f, 1800);
      aua1.signalPartialAgreement(pid2, POP, .75f, 1800);

      aua1.signalPartialAgreement(pid3, POR, .30f, 1900);
      aua1.signalPartialAgreement(pid3, POP, .55f, 1900);

      aua2.signalPartialAgreement(pid2, POR, .15f, 1910);
      aua2.signalPartialAgreement(pid2, POP, .25f, 1910);
      storeAuAgreements(aua1, pid2, pid3);
      storeAuAgreements(aua2, pid2);
      assertAua2();
    }

    void assertAua2() {
      assertAgreeTime(.95f, 1800, aua1.findPeerAgreement(pid2, POR));
      assertAgreeTime(.75f, 1800, aua1.findPeerAgreement(pid2, POP));
      assertAgreeTime(.30f, 1900, aua1.findPeerAgreement(pid3, POR));
      assertAgreeTime(.55f, 1900, aua1.findPeerAgreement(pid3, POP));
      assertAgreeTime(.15f, 1910, aua2.findPeerAgreement(pid2, POR));
      assertAgreeTime(.25f, 1910, aua2.findPeerAgreement(pid2, POP));

      assertSame(aua1, stateMgr.getAuAgreements(gauid1));
    }

  }

}
