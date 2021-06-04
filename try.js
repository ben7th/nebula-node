const GraphTtypes = require('./nebula/interface/graph_types')
const NebulaConn = require("./nebula/net/NebulaConn").NebulaConn

const connAuthenticate = async ({ conn, user, password }) => {
  return new Promise((resolve, reject) => {
    conn.authenticate(user, password, (res) => {
      resolve(res)
    })
  })
}

const connExecute = async ({ conn, sessionId, cmd }) => {
  return new Promise((resolve, reject) => {
    conn.execute(sessionId, cmd, (res) => {
      resolve(res)
    })
  })
}

const withSession = async ({ conn, user, password }, func) => {
  conn.open()
  let res0 = await connAuthenticate({ conn, user: 'user', password: 'password' })
  let sessionId = res0.success.session_id
  let executer = new Executer({ conn, sessionId })
  await func({ 
    execute: executer.execute.bind(executer),
    executeData: executer.executeData.bind(executer)
  })
  conn.close()
}

class Executer {
  constructor ({ conn, sessionId }) {
    this.conn = conn
    this.sessionId = sessionId
  }

  async execute (cmd) {
    return await connExecute({ 
      conn: this.conn, 
      sessionId: this.sessionId, 
      cmd
    })
  }

  async executeData (cmd) {
    let res = await this.execute(cmd)

    let { column_names, rows } = res.success.data
    let items = rows.map(row => {
      let values = row.values
      let data = column_names.map((column_name, idx) => {
        let valueList = Object.values(values[idx])
        let value = valueList.filter(v => !!v)[0] || null
        return [ column_name, value ]
      })
      return data
    })
    return items
  }
}

const run = async () => {
  const conn = new NebulaConn('localhost', 9669, 1000)
  try {
    withSession({ conn, user: 'user', password: 'password' }, async ({ execute, executeData })=> {
      let res1 = await execute('USE slime')
      // console.log(res1)
      let res2 = await execute('CREATE TAG friend (name string NOT NULL)')
      // console.log(res2)
      let res3 = await executeData('DESC TAG friend')
      console.log(res3)
    })
  } catch (e) {
    conn.close()
    console.log(e.stack)
  } 
}

run().then()