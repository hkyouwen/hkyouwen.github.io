本文基于flink-1.10版本分析flink命令行提交任务的大致过程，主要是看run命令。flink-1.10主要变化有增加ExecutorCLI，k8s session提交使用ExecutorCLI；移除runOptions，引入programOptions；移除runProgram方法；移除executeProgram的JobSubmissionResult 返回值等

flink的命令行提交如下所示：

```
/app/flink/bin/flink run -m yarn-cluster -ytm 2048 -ynm flink-test -yqu flink-queue -ys 1 flink-test.jar
```

flink脚本调用flink客户端的代码：

```
exec $JAVA_RUN $JVM_ARGS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.client.cli.CliFrontend "$@"
```

进入CliFrontend的main方法：

```
public static void main(final String[] args) {
   EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);

   // 1. find the configuration directory
   final String configurationDirectory = getConfigurationDirectoryFromEnv();

   // 2. load the global configuration
   //获取配置文件目录：.../flink1.10.0/conf
   final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

   // 3. load the custom command lines
   //加载flink-conf.yaml中的全局配置转成Configuration对象
   //CustomCommandLine有三种实现，依此添加进customCommandLines，分别是ExecutorCLI（k8ssession提交使用ExecutorCLI），FlinkYarnSessionCli和DefaultCLI
   final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
      configuration,
      configurationDirectory);

   try {
      final CliFrontend cli = new CliFrontend(
         configuration,
         customCommandLines);

//flink全局安全配置
      SecurityUtils.install(new SecurityConfiguration(cli.configuration));
      //解析命令行
      int retCode = SecurityUtils.getInstalledContext()
            .runSecured(() -> cli.parseParameters(args));
      //获取执行返回值，关闭提交程序
      System.exit(retCode);
   }
}
```

loadCustomCommandLines添加三种CLI入口

```
public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
   List<CustomCommandLine> customCommandLines = new ArrayList<>();
   customCommandLines.add(new ExecutorCLI(configuration));

   // Command line interface of the YARN session, with a special initialization here
   // to prefix all options with y/yarn.
   final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
   try {
      customCommandLines.add(
         loadCustomCommandLine(flinkYarnSessionCLI,
            configuration,
            configurationDirectory,
            "y",
            "yarn"));
   } catch (NoClassDefFoundError | Exception e) {
      LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
   }

   // Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
   //       active CustomCommandLine in order and DefaultCLI isActive always return true.
   customCommandLines.add(new DefaultCLI(configuration));

   return customCommandLines;
}
```

parseParameters解析命令行并根据action来执行不同的操作：

```
public int parseParameters(String[] args) {

   // check for action
   if (args.length < 1) {
      CliFrontendParser.printHelp(customCommandLines);
      System.out.println("Please specify an action.");
      return 1;
   }

   // get action
   String action = args[0];

   // remove action from parameters
   final String[] params = Arrays.copyOfRange(args, 1, args.length);

   try {
      // do action
      switch (action) {
         case ACTION_RUN:
            run(params);
            return 0;
         case ACTION_LIST:
            list(params);
            return 0;
         case ACTION_INFO:
            info(params);
            return 0;
         case ACTION_CANCEL:
            cancel(params);
            return 0;
         case ACTION_STOP:
            stop(params);
            return 0;
         case ACTION_SAVEPOINT:
            savepoint(params);
            return 0;
         case "-h":
         case "--help":
            CliFrontendParser.printHelp(customCommandLines);
            return 0;
         case "-v":
         case "--version":
            String version = EnvironmentInformation.getVersion();
            String commitID = EnvironmentInformation.getRevisionInformation().commitId;
            System.out.print("Version: " + version);
            System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
            return 0;
         default:
            System.out.printf("\"%s\" is not a valid action.\n", action);
            System.out.println();
            System.out.println("Valid actions are \"run\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
            System.out.println();
            System.out.println("Specify the version option (-v or --version) to print Flink version.");
            System.out.println();
            System.out.println("Specify the help option (-h or --help) to get help on the command.");
            return 1;
      }
   } catch (CliArgsException ce) {
      return handleArgException(ce);
   } catch (ProgramParametrizationException ppe) {
      return handleParametrizationException(ppe);
   } catch (ProgramMissingJobException pmje) {
      return handleMissingJobException();
   } catch (Exception e) {
      return handleError(e);
   }
}
```

以提交任务的run命令为例，对应CliFrontend类run方法，需要说明的是flink1.10的run方法与之前版本差别比较大，移除了runProgram方法，直接替换新的executeProgram方法，简化了之前yarn集群创建与提交的过程：

```
protected void run(String[] args) throws Exception {
   LOG.info("Running 'run' command.");

   //解析命令行参数
   final Options commandOptions = CliFrontendParser.getRunCommandOptions();
   final CommandLine commandLine = getCommandLine(commandOptions, args, true);

   //判断jobs是python还是java，并获取提取参数信息
   final ProgramOptions programOptions = new ProgramOptions(commandLine);

//jar包不能为null
   if (!programOptions.isPython()) {
      // Java program should be specified a JAR file
      if (programOptions.getJarFilePath() == null) {
         throw new CliArgsException("Java program should be specified a JAR file.");
      }
   }

   final PackagedProgram program;
   try {
      LOG.info("Building program from JAR file");
      //通过jar包获取class路径，入口类等信息并构建PackagedProgram程序类，主要过程：寻找程序入口，解析客户代码获取任务拓扑图，提取嵌套库
      program = buildProgram(programOptions);
   }
   catch (FileNotFoundException e) {
      throw new CliArgsException("Could not build the program from JAR file.", e);
   }

   final List<URL> jobJars = program.getJobJarAndDependencies();
   final Configuration effectiveConfiguration =
         getEffectiveConfiguration(commandLine, programOptions, jobJars);

   LOG.debug("Effective executor configuration: {}", effectiveConfiguration);

   try {
   //主要逻辑，与Jobmanager和programs交互
      executeProgram(effectiveConfiguration, program);
   } finally {
      program.deleteExtractedLibraries();
   }
}
```

通过命令行参数得到的配置及jar包和程序代码传入ClientUtils类的executeProgram

```
public static void executeProgram(
			PipelineExecutorServiceLoader executorServiceLoader,
			Configuration configuration,
			PackagedProgram program) throws ProgramInvocationException {
		checkNotNull(executorServiceLoader);
		final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);

			LOG.info("Starting program (detached: {})", !configuration.getBoolean(DeploymentOptions.ATTACHED));

//构建通过命令行提交的flink任务执行的环境上下文
			ContextEnvironmentFactory factory = new ContextEnvironmentFactory(
					executorServiceLoader,
					configuration,
					userCodeClassLoader);
			ContextEnvironment.setAsContext(factory);

			try {
			//执行到这个方法意味着上下文环境已经准备好，或者是默认本地执行
				program.invokeInteractiveModeForExecution();
			} finally {
				ContextEnvironment.unsetContext();
			}
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
```

执行代码的main方法，通过PackagedProgram类的callMainMethod方法，mainMethod.invoke执行main函数，进入三层DAG的转化及集群启动逻辑

```
private static void callMainMethod(Class<?> entryClass, String[] args) throws ProgramInvocationException {
   Method mainMethod;
   ......(判断main方法是public，static的)

   try {
      mainMethod.invoke(null, (Object) args);
   } catch (IllegalArgumentException e) {
      throw new ProgramInvocationException("Could not invoke the main method, arguments are not matching.", e);
   } catch (IllegalAccessException e) {
      throw new ProgramInvocationException("Access to the main method was denied: " + e.getMessage(), e);
   } catch (InvocationTargetException e) {
      Throwable exceptionInMethod = e.getTargetException();
      if (exceptionInMethod instanceof Error) {
         throw (Error) exceptionInMethod;
      } else if (exceptionInMethod instanceof ProgramParametrizationException) {
         throw (ProgramParametrizationException) exceptionInMethod;
      } else if (exceptionInMethod instanceof ProgramInvocationException) {
         throw (ProgramInvocationException) exceptionInMethod;
      } else {
         throw new ProgramInvocationException("The main method caused an error: " + exceptionInMethod.getMessage(), exceptionInMethod);
      }
   } catch (Throwable t) {
      throw new ProgramInvocationException("An error occurred while invoking the program's main method: " + t.getMessage(), t);
   }
}
```