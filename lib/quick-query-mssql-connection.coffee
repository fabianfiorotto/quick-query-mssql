sql = require 'mssql'

{Emitter} = require 'atom'

class QuickQueryMssqlColumn
  type: 'column'
  child_type: null
  constructor: (@table,row) ->
    @connection = @table.connection
    @name = row['COLUMN_NAME']
    @primary_key = row['constraint_type'] == 'PRIMARY KEY'
    if row['CHARACTER_MAXIMUM_LENGTH']
      @datatype = "#{row['DATA_TYPE']} (#{row['CHARACTER_MAXIMUM_LENGTH']})"
    else
      @datatype = row['DATA_TYPE']
    @default = row['COLUMN_DEFAULT']
    if @default == 'NULL' || @default == "NULL::#{row['DATA_TYPE']}"
      @default = null
    if @default != null
      while @default[0] == "("
        @default = @default.substring(1,@default.length-1)
    @nullable = row['IS_NULLABLE'] == 'YES'
    @id = parseInt(row['ORDINAL_POSITION'])
  toString: ->
    @name
  parent: ->
    @table
  children: (callback)->
    callback([])

class QuickQueryMssqlTable
  type: 'table'
  child_type: 'column'
  constructor: (@schema,row,fields) ->
    @connection = @schema.connection
    @name = row['table_name']
  toString: ->
    @name
  parent: ->
    @schema
  children: (callback)->
    @connection.getColumns(@,callback)
class QuickQueryMssqlSchema
  type: 'schema'
  child_type: 'table'
  constructor: (@database,row,fields) ->
    @connection = @database.connection
    @name = row["schema_name"]
  toString: ->
    @name
  parent: ->
    @database
  children: (callback)->
    @connection.getTables(@,callback)
class QuickQueryMssqlDatabase
  type: 'database'
  child_type: 'schema'
  constructor: (@connection,row) ->
    @name = row["name"]
  toString: ->
    @name
  parent: ->
    @connection
  children: (callback)->
    @connection.getSchemas(@,callback)

module.exports =
class QuickQueryMssqlConnection

  fatal: false
  connection: null
  protocol: 'mssql'
  type: 'connection'
  child_type: 'database'

  n_types: 'bigint numeric bit smallint decimal smallmoney int tinyint money float'.split /\s+/
  s_types: 'datetimeoffset datetime2 smalldatetime datetime date nchar ntext nvarchar char text varchar'.split /\s+/

  allowEdition: false
  @defaultPort: 1433

  constructor: (@info)->
    @info.server = @info.host
    @info.database ?= "master"
    @emitter = new Emitter()

  connect: (callback)->
    @connection = new sql.Connection @info , (err) ->
      callback(err)

  serialize: ->
    c = @connection.config
    host: c.host,
    port: c.port,
    protocol: @protocol
    database: c.database,
    user: c.user,
    password: c.password

  dispose: ->
    @close()

  close: ->
    @connection.close()

  query: (text,callback) ->
    request = @connection.request()
    request.query text, (err, recordset) =>
      if (err)
        message = { type: 'error' , content: err.toString() }
        callback(message,rows,fields)
      else if recordset?.columns?
        fields = []
        rows = []
        for column_name,column of recordset.columns
          fields.push(column)
        @prepareRow(row,fields) for row in recordset
        callback(null,recordset,fields)
      else
        request = @connection.request()
        request.query "SELECT @@ROWCOUNT AS rows_affected", (err, recordset) =>
          console.log recordset
          if !err && recordset? && recordset.length == 1
            callback type: 'success', content:  "#{recordset[0].rows_affected} row(s) affected"
          else
            callback type: 'success', content: "Success"

  prepareRow: (row,fields)->
    for field in fields
      if field.type.declaration == 'date'
        row[field.name] = row[field.name].toISOString().slice(0, 10) if row[field.name]?
      else if field.type.declaration == 'time'
        row[field.name] = row[field.name].toISOString().slice(11, 23) if row[field.name]?
      else if ['datetime','datetime2','smalldatetime'].indexOf(field.type.declaration) != -1 && row[field.name]?
        row[field.name] = row[field.name].toISOString().slice(0,10)+" "+row[field.name].toISOString().slice(11,23)

          # row[field.name] = row[field.name].toISOString() if row[field.name]?


  setDefaultDatabase: (database)->
    connection = @connection
    @info.database = database
    @connect (err)=>
      if !err
        connection.close()
        @emitter.emit 'did-change-default-database', @connection.config.database
      else
        @connecton = connection
        console.log(err)

  getDefaultDatabase: ->
    @connection.config.database

  parent: -> @

  children: (callback)->
    @getDatabases (databases,err)->
      unless err? then callback(databases) else console.log err

  getDatabases: (callback) ->
    text = "SELECT name FROM master.dbo.sysdatabases"
    @query text , (err, rows, fields) =>
      if !err
        databases = rows.map (row) =>
          new QuickQueryMssqlDatabase(@,row)
        databases = databases.filter (database) => !@hiddenDatabase(database.name)
      callback(databases,err)

  getSchemas: (database, callback)->
    database_name = @escapeId(database.name)
    text = "SELECT schema_name FROM #{database_name}.information_schema.schemata"+
    " WHERE schema_name NOT IN ('INFORMATION_SCHEMA','SYS', 'NT AUTHORITY\\NETWORK SERVICE') "+
    " AND schema_name NOT IN ('db_owner','db_accessadmin','db_securityadmin','db_ddladmin','db_backupoperator','db_datareader','db_datawriter','db_denydatareader','db_denydatawriter')"
    @query text, (err, rows, fields) =>
      if !err
        schemas = rows.map (row) ->
          new QuickQueryMssqlSchema(database,row)
        callback(schemas)

  getTables: (schema,callback) ->
    database_name = @escapeId(schema.database.name)
    text = "SELECT table_name FROM #{database_name}.information_schema.tables"+
    " WHERE table_schema = '#{schema.name}' "+
    " AND table_catalog = '#{schema.database.name}'"+
    " ORDER BY table_name"
    @query text , (err, rows, fields) =>
      if !err
        tables = rows.map (row) =>
          new QuickQueryMssqlTable(schema,row,fields)
        callback(tables)

  getColumns: (table,callback) ->
    database_name = @escapeId(table.schema.database.name)
    text = "SELECT  pk.constraint_type ,c.*"+
    " FROM #{database_name}.information_schema.columns c"+
    " LEFT OUTER JOIN ("+
    "  SELECT"+
    "   tc.constraint_type,"+
    "   kc.column_name,"+
    "   tc.table_catalog,"+
    "   tc.table_name,"+
    "   tc.table_schema"+
    "  FROM #{database_name}.information_schema.table_constraints tc"+
    "  INNER JOIN #{database_name}.information_schema.CONSTRAINT_COLUMN_USAGE kc"+
    "  ON kc.constraint_name = tc.constraint_name"+
    "  AND kc.table_catalog = tc.table_catalog"+
    "  AND kc.table_name = tc.table_name"+
    "  AND kc.table_schema = tc.table_schema"+
    "  WHERE tc.constraint_type = 'PRIMARY KEY'"+
    " ) pk ON pk.column_name = c.column_name"+
    "  AND pk.table_catalog = c.table_catalog"+
    "  AND pk.table_name = c.table_name"+
    "  AND pk.table_schema = c.table_schema"+
    " WHERE c.table_name = '#{table.name}' "+
    " AND c.table_schema = '#{table.schema.name}' "+
    " AND c.table_catalog = '#{table.schema.database.name}'"
    @query text , (err, rows, fields) =>
      if !err
        columns = rows.map (row) =>
          new QuickQueryMssqlColumn(table,row)
        callback(columns)
      else
        console.log(err)

  hiddenDatabase: (database) ->
    database == "master" ||
    database == "tempdb" ||
    database == "model"  ||
    database == "msdb"

  simpleSelect: (table, columns = '*') ->
    if columns != '*'
      columns = columns.map (col) =>
        @escapeId(col.name)
      columns = "\n "+columns.join(",\n ") + "\n"
    table_name = @escapeId(table.name)
    schema_name = @escapeId(table.schema.name)
    database_name = @escapeId(table.schema.database.name)
    "SELECT TOP 1000 #{columns} FROM #{database_name}.#{schema_name}.#{table_name}"


  createDatabase: (model,info)->
    database = @escapeId(info.name)
    "CREATE DATABASE #{database};"

  createSchema: (model,info)->
    schema = @escapeId(info.name)
    "CREATE SCHEMA #{schema};"

  createTable: (model,info)->
    database = @escapeId(model.database.name)
    schema = @escapeId(model.name)
    table = @escapeId(info.name)
    "CREATE TABLE #{database}.#{schema}.#{table}(\n"+
    " [id] int PRIMARY KEY NOT NULL\n"+
    ")"

  createColumn: (model,info)->
    database = @escapeId(model.schema.database.name)
    schema = @escapeId(model.schema.name)
    table = @escapeId(model.name)
    column = @escapeId(info.name)
    nullable = if info.nullable then 'NULL' else 'NOT NULL'
    dafaultValue = @escape(info.default,info.datatype) || 'NULL'
    "ALTER TABLE #{database}.#{schema}.#{table} ADD #{column}"+
    " #{info.datatype} #{nullable} DEFAULT #{dafaultValue};"


  alterTable: (model,delta)->
    database = @escapeId(model.schema.database.name)
    schema = model.schema.name
    newName = delta.new_name
    oldName = delta.old_name
    query = "USE #{database}\nsp_rename '#{schema}.#{oldName}' , '#{newName}';"

  alterColumn: (model,delta)->
    database = @escapeId(model.table.database.name)
    table = model.table.name
    newName = delta.new_name
    oldName = delta.old_name
    nullable = if delta.nullable then 'NULL' else 'NOT NULL'
    dafaultValue = @escape(delta.default,delta.datatype) || 'NULL'
    result = "ALTER TABLE #{database}.#{table} ALTER COLUMN #{oldName}"+
    " #{delta.datatype} #{nullable} DEFAULT #{dafaultValue};"
    if oldName != newName
      result += "\nUSE #{database}\nsp_rename '#{schema}.#{table}.#{oldName}' ,"+
      "'#{newName}', 'COLUMN';"
    result

  dropDatabase: (model)->
    database = @escapeId(model.name)
    "DROP SCHEMA #{database};"

  dropTable: (model)->
    database = @escapeId(model.schema.database.name)
    schema = @escapeId(model.schema.name)
    table = @escapeId(model.name)
    "DROP TABLE #{database}.#{schema}.#{table}"

  dropColumn: (model)->
    database = @escapeId(model.table.schema.database.name)
    schema = @escapeId(model.table.schema.name)
    table = @escapeId(model.table.name)
    column = @escapeId(model.name)
    "ALTER TABLE #{database}.#{schema}.#{table} DROP COLUMN #{column};"

  sentenceReady: (callback)->
    @emitter.on 'sentence-ready', callback

  onDidChangeDefaultDatabase: (callback)->
    @emitter.on 'did-change-default-database', callback

  getDataTypes: ->
    @n_types.concat(@s_types)

  toString: ->
    @protocol+"://"+@connection.config.user+"@"+@connection.config.host

  escapeId: (str)->
    "[#{str}]"

  escape: (value,type)->
    for t1 in @s_types
      if value == null || type.search(new RegExp(t1, "i")) != -1
        if t1 == 'nvarchar' || t1 == 'ntext' || t1 == 'nchar'
          return "N'#{value}'"
        else
          return "'#{value}'"
    value.toString()
