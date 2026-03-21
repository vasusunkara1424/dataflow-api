const { LogicalReplicationService, PgoutputPlugin } = require('pg-logical-replication');

const service = new LogicalReplicationService({
  database: 'dataflow_db',
  host: 'localhost',
  port: 5432,
});

const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames: ['datagrid_pub'],
});

console.log('👂 CDC listener starting...');

service.on('data', (lsn, log) => {
  if (log.tag === 'insert') {
    console.log('✅ INSERT detected:', log.relation.name, log.new);
  }
  if (log.tag === 'update') {
    console.log('✏️  UPDATE detected:', log.relation.name, log.new);
  }
  if (log.tag === 'delete') {
    console.log('🗑️  DELETE detected:', log.relation.name, log.old);
  }
});

service.subscribe(plugin, 'datagrid_slot');
