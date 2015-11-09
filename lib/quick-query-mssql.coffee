QuickQueryMssqlConnection = require './quick-query-mssql-connection'

{CompositeDisposable} = require 'atom'

module.exports = QuickQueryMssql =
  subscriptions: null

  activate: (state) ->
    @subscriptions = new CompositeDisposable

  deactivate: ->
    @subscriptions.dispose()


  consumeConnectView: (connectView)->
    protocol =
      name: 'SQL Server'
      handler: QuickQueryMssqlConnection
    connectView.addProtocol('mssql',protocol)

  serialize: ->
