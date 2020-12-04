zeppelin 0.9版本支持引入了对sql的支持，可以直接在notebook通过DDL语句创建输入和输出的table。

![](640.png)

简单看了一下zeppelin把sql翻译成flink代码的原理。zeppelin中的flink的interpreter支持多种flink的api:

![](1.png)

其中%flink.bsql和%flink.ssql对应sql类型的interpreter，以bsql为例，bsql的入口类为org.apache.zeppelin.flink.FlinkBatchSqlInterpreter。

```
public class FlinkBatchSqlInterpreter extends FlinkSqlInterrpeter 
```

FlinkBatchSqlInterpreter继承自FlinkSqlInterrpeter，所以主要解析逻辑在FlinkSqlInterrpeter

```
public InterpreterResult interpret(String st,
                                     InterpreterContext context) throws InterpreterException {
    LOGGER.debug("Interpret code: " + st);
    flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    flinkInterpreter.getZeppelinContext().setGui(context.getGui());

    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(flinkInterpreter.getFlinkScalaShellLoader());
      return runSqlList(st, context);
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader);
    }
  }
```

FlinkSqlInterrpeter的interpret方法为翻译入口，入参st即为notebook上键入的sql字符串。st被传入runSqlList方法。

```
private InterpreterResult runSqlList(String st, InterpreterContext context) {
    currentConfigOptions.clear();
    List<String> sqls = sqlSplitter.splitSql(st);
    for (String sql : sqls) {
      Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse(sql);
      ......
      try {
        callCommand(sqlCommand.get(), context);
        context.out.flush();
      } catch (Throwable e) {
        ......
    }
```

runSqlList将st分割成子语句并使用SqlCommandParser.parse解析语句，根据解析出的command类型如（CREATE TABLE，INSERT_INTO等）进行逻辑执行（callCommand）。

```
public List<String> splitSql(String text) {
    text = text.trim();
    List<String> queries = new ArrayList<>();
    StringBuilder query = new StringBuilder();
    char character;

    boolean multiLineComment = false;
    boolean singleLineComment = false;
    boolean singleQuoteString = false;
    boolean doubleQuoteString = false;

    for (int index = 0; index < text.length(); index++) {
      character = text.charAt(index);

      // end of single line comment
      if (singleLineComment && (character == '\n')) {
        singleLineComment = false;
        if (query.toString().trim().isEmpty()) {
          continue;
        }
      }

      // end of multiple line comment
      if (multiLineComment && character == '/' && text.charAt(index - 1) == '*') {
        multiLineComment = false;
        if (query.toString().trim().isEmpty()) {
          continue;
        }
      }

      if (character == '\'') {
        if (singleQuoteString) {
          singleQuoteString = false;
        } else if (!doubleQuoteString) {
          singleQuoteString = true;
        }
      }

      if (character == '"') {
        if (doubleQuoteString && index > 0) {
          doubleQuoteString = false;
        } else if (!singleQuoteString) {
          doubleQuoteString = true;
        }
      }

      if (!singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment
              && text.length() > (index + 1)) {
        if (isSingleLineComment(text.charAt(index), text.charAt(index + 1))) {
          singleLineComment = true;
        } else if (text.charAt(index) == '/' && text.charAt(index + 1) == '*') {
          multiLineComment = true;
        }
      }

      if (character == ';' && !singleQuoteString && !doubleQuoteString && !multiLineComment
              && !singleLineComment) {
        // meet semicolon
        queries.add(query.toString().trim());
        query = new StringBuilder();
      } else if (index == (text.length() - 1)) {
        // meet the last character
        if (!singleLineComment && !multiLineComment) {
          query.append(character);
          queries.add(query.toString().trim());
        }
      } else if (!singleLineComment && !multiLineComment) {
        // normal case, not in single line comment and not in multiple line comment
        query.append(character);
      } else if (singleLineComment && !query.toString().trim().isEmpty()) {
        // in single line comment, only add it to query when the single line comment is
        // in the middle of sql statement
        // e.g.
        // select a -- comment
        // from table_1
        query.append(character);
      } else if (multiLineComment && !query.toString().trim().isEmpty()) {
        // in multiple line comment, only add it to query when the multiple line comment
        // is in the middle of sql statement.
        // e.g.
        // select a /* comment */
        // from table_1
        query.append(character);
      }
    }

    return queries;
    }
```

st的分割过程，可以看注释内容，主要是逐个字符的遍历及'\n'和';'及引号等符号还有注释内容的逻辑判断，最终得到各个自语句`List<String> queries`。

```
public static Optional<SqlCommandCall> parse(String stmt) {
    // normalize
    stmt = stmt.trim();
    // remove ';' at the end
    if (stmt.endsWith(";")) {
      stmt = stmt.substring(0, stmt.length() - 1).trim();
    }

    // parse
    for (SqlCommand cmd : SqlCommand.values()) {
      final Matcher matcher = cmd.pattern.matcher(stmt);
      if (matcher.matches()) {
        final String[] groups = new String[matcher.groupCount()];
        for (int i = 0; i < groups.length; i++) {
          groups[i] = matcher.group(i + 1);
        }
        final String sql = stmt;
        return cmd.operandConverter.apply(groups)
                .map((operands) -> new SqlCommandCall(cmd, operands, sql));
      }
    }
    return Optional.empty();
  }
```

逐个子语句解析，解析过程也比较简单，就是遍历各种SqlCommand，然后用正则表达式进行匹配看子语句中包含哪个类型的SqlCommand，。然后得到一个SqlCommandCall的类，包含命令名，被操作字符串及sql语句。

其中被操作字符串operands由operandConverter来提取，分为NO_OPERANDS和SINGLE_OPERAND：

```
private static final Function<String[], Optional<String[]>> NO_OPERANDS =
          (operands) -> Optional.of(new String[0]);

  private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
          (operands) -> Optional.of(new String[]{operands[0]});
```

NO_OPERANDS即不需要操作sql的命令，如HELP，QUIT，CLEAR等，SINGLE_OPERAND为操作单个字符串的命令，如CREATE TABLE，INSERT_INTO等，更复杂的命令要单独提取，如SET。命令的正则表达式及operandConverter示例如下所示：

```
public enum SqlCommand {
    QUIT(
            "(QUIT|EXIT)",
            NO_OPERANDS),

    CLEAR(
            "CLEAR",
            NO_OPERANDS),

   ......

    CREATE_TABLE("(CREATE\\s+TABLE\\s+.*)", SINGLE_OPERAND),

    DROP_TABLE("(DROP\\s+TABLE\\s+.*)", SINGLE_OPERAND),

    CREATE_VIEW(
            "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)",
            (operands) -> {
              if (operands.length < 2) {
                return Optional.empty();
              }
              return Optional.of(new String[]{operands[0], operands[1]});
            }),

    ......

    SET(
            "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
            (operands) -> {
              if (operands.length < 3) {
                return Optional.empty();
              } else if (operands[0] == null) {
                return Optional.of(new String[0]);
              }
              return Optional.of(new String[]{operands[1], operands[2]});
            }),

    public final Pattern pattern;
    public final Function<String[], Optional<String[]>> operandConverter;

    SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
      this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
      this.operandConverter = operandConverter;
    }
```

解析完之后进入runSqlList的callCommand方法执行命令

```
 private void callCommand(SqlCommandParser.SqlCommandCall cmdCall,
                                        InterpreterContext context) throws Exception {
    switch (cmdCall.command) {
      case HELP:
        callHelp(context);
        break;
      case SHOW_CATALOGS:
        callShowCatalogs(context);
        break;
      case SHOW_DATABASES:
        callShowDatabases(context);
        break;
      case SHOW_TABLES:
        callShowTables(context);
        break;
      case SOURCE:
        callSource(cmdCall.operands[0], context);
        break;
      case SHOW_FUNCTIONS:
        callShowFunctions(context);
        break;
      case SHOW_MODULES:
        callShowModules(context);
        break;
      case USE_CATALOG:
        callUseCatalog(cmdCall.operands[0], context);
        break;
      case USE:
        callUseDatabase(cmdCall.operands[0], context);
        break;
      case DESCRIBE:
        callDescribe(cmdCall.operands[0], context);
        break;
      case EXPLAIN:
        callExplain(cmdCall.operands[0], context);
        break;
      case SELECT:
        callSelect(cmdCall.operands[0], context);
        break;
      case SET:
        callSet(cmdCall.operands[0], cmdCall.operands[1], context);
        break;
      case INSERT_INTO:
      case INSERT_OVERWRITE:
        callInsertInto(cmdCall.operands[0], context);
        break;
      case CREATE_TABLE:
        callCreateTable(cmdCall.operands[0], context);
        break;
      case DROP_TABLE:
        callDropTable(cmdCall.operands[0], context);
        break;
      case CREATE_VIEW:
        callCreateView(cmdCall.operands[0], cmdCall.operands[1], context);
        break;
      case DROP_VIEW:
        callDropView(cmdCall.operands[0], context);
        break;
      case CREATE_DATABASE:
        callCreateDatabase(cmdCall.operands[0], context);
        break;
      case DROP_DATABASE:
        callDropDatabase(cmdCall.operands[0], context);
        break;
      case ALTER_DATABASE:
        callAlterDatabase(cmdCall.operands[0], context);
        break;
      case ALTER_TABLE:
        callAlterTable(cmdCall.operands[0], context);
        break;
      default:
        throw new Exception("Unsupported command: " + cmdCall.command);
    }
  }
```

根据命令名调用不同的api。如CREATE TABLE命令调用callCreateTable

```
private void callCreateTable(String sql, InterpreterContext context) throws IOException {
    try {
      lock.lock();
      this.tbenv.sqlUpdate(sql);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
    context.out.write("Table has been created.\n");
  }
```

直接调用sqlUpdate api，则翻译成了flink1.10的flink sql代码。

如上CREATE TABLE为通用逻辑，而SELECT则区分batch和stream

```
public void callSelect(String sql, InterpreterContext context) throws IOException {
    try {
      ......
      callInnerSelect(sql, context);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    ......
  }

  public abstract void callInnerSelect(String sql, InterpreterContext context) throws IOException;
```

callInnerSelect为抽象方法

```
public void callInnerSelect(String sql, InterpreterContext context) throws IOException {
    Table table = this.tbenv.sqlQuery(sql);
    z.setCurrentSql(sql);
    String result = z.showData(table);
    context.out.write(result);
  }
```

上面为FlinkBatchSqlInterpreter的实现，比较简单直接调用sqlQuery即可

```
public void callInnerSelect(String sql, InterpreterContext context) throws IOException {
    String savepointDir = context.getLocalProperties().get("savepointDir");
    if (!StringUtils.isBlank(savepointDir)) {
      Object savepointPath = flinkInterpreter.getZeppelinContext()
              .angular(context.getParagraphId() + "_savepointpath", context.getNoteId(), null);
      if (savepointPath == null) {
        LOGGER.info("savepointPath is null because it is the first run");
      } else {
        LOGGER.info("set savepointPath to: " + savepointPath.toString());
        this.flinkInterpreter.getFlinkConfiguration()
                .setString("execution.savepoint.path", savepointPath.toString());
      }
    }

    String streamType = context.getLocalProperties().get("type");
    if (streamType == null) {
      throw new IOException("type must be specified for stream sql");
    }
    if (streamType.equalsIgnoreCase("single")) {
      SingleRowStreamSqlJob streamJob = new SingleRowStreamSqlJob(
              flinkInterpreter.getStreamExecutionEnvironment(),
              tbenv,
              flinkInterpreter.getJobManager(),
              context,
              flinkInterpreter.getDefaultParallelism());
      streamJob.run(sql);
    } else if (streamType.equalsIgnoreCase("append")) {
      AppendStreamSqlJob streamJob = new AppendStreamSqlJob(
              flinkInterpreter.getStreamExecutionEnvironment(),
              flinkInterpreter.getStreamTableEnvironment(),
              flinkInterpreter.getJobManager(),
              context,
              flinkInterpreter.getDefaultParallelism());
      streamJob.run(sql);
    } else if (streamType.equalsIgnoreCase("update")) {
      UpdateStreamSqlJob streamJob = new UpdateStreamSqlJob(
              flinkInterpreter.getStreamExecutionEnvironment(),
              flinkInterpreter.getStreamTableEnvironment(),
              flinkInterpreter.getJobManager(),
              context,
              flinkInterpreter.getDefaultParallelism());
      streamJob.run(sql);
    } else {
      throw new IOException("Unrecognized stream type: " + streamType);
    }
  }
```

FlinkStreamSqlInterpreter的实现则更复杂，分append，update等模式，后面再研究。

附上现有的SqlCommand类型

```
public static final AttributedString MESSAGE_HELP = new AttributedStringBuilder()
          .append("The following commands are available:\n\n")
          .append(formatCommand(SqlCommand.CREATE_TABLE, "Create table under current catalog and database."))
          .append(formatCommand(SqlCommand.DROP_TABLE, "Drop table with optional catalog and database. Syntax: 'DROP TABLE [IF EXISTS] <name>;'"))
          .append(formatCommand(SqlCommand.CREATE_VIEW, "Creates a virtual table from a SQL query. Syntax: 'CREATE VIEW <name> AS <query>;'"))
          .append(formatCommand(SqlCommand.DESCRIBE, "Describes the schema of a table with the given name."))
          .append(formatCommand(SqlCommand.DROP_VIEW, "Deletes a previously created virtual table. Syntax: 'DROP VIEW <name>;'"))
          .append(formatCommand(SqlCommand.EXPLAIN, "Describes the execution plan of a query or table with the given name."))
          .append(formatCommand(SqlCommand.HELP, "Prints the available commands."))
          .append(formatCommand(SqlCommand.INSERT_INTO, "Inserts the results of a SQL SELECT query into a declared table sink."))
          .append(formatCommand(SqlCommand.INSERT_OVERWRITE, "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data."))
          .append(formatCommand(SqlCommand.SELECT, "Executes a SQL SELECT query on the Flink cluster."))
          .append(formatCommand(SqlCommand.SET, "Sets a session configuration property. Syntax: 'SET <key>=<value>;'. Use 'SET;' for listing all properties."))
          .append(formatCommand(SqlCommand.SHOW_FUNCTIONS, "Shows all user-defined and built-in functions."))
          .append(formatCommand(SqlCommand.SHOW_TABLES, "Shows all registered tables."))
          .append(formatCommand(SqlCommand.SOURCE, "Reads a SQL SELECT query from a file and executes it on the Flink cluster."))
          .append(formatCommand(SqlCommand.USE_CATALOG, "Sets the current catalog. The current database is set to the catalog's default one. Experimental! Syntax: 'USE CATALOG <name>;'"))
          .append(formatCommand(SqlCommand.USE, "Sets the current default database. Experimental! Syntax: 'USE <name>;'"))
          .style(AttributedStyle.DEFAULT.underline())
          .append("\nHint")
          .style(AttributedStyle.DEFAULT)
          .append(": Make sure that a statement ends with ';' for finalizing (multi-line) statements.")
          .toAttributedString();
```

