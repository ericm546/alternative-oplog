Package.describe({
    name: 'ericmartin989:alternative-oplog',
  summary: 'Alternative oplog tailing for Meteor',
  version: '0.0.1',
   git: 'https://github.com/ericm546/alternative-oplog.git'
});

Npm.depends({
  'mongodb-uri': '0.9.7',
  'lodash.isempty': '4.4.0',
  'lodash.has': '4.5.2',
  'lodash.throttle': '4.1.1',
  'lodash.isobject': '3.0.2',
  'lodash.clone': '4.5.0',
});

Package.onUse(function (api) {
  api.versionsFrom(["3.0.1", "3.1", "3.2"]);
  api.use('npm-mongo', 'server');
  api.use('allow-deny');
  api.use([
    'random',
    'ejson',
    'minimongo',
    'ddp',
    'tracker',
    'diff-sequence',
    'mongo-id',
    'check',
    'ecmascript',
    'typescript',
    'mongo-dev-server',
    'logging',
  ]);
  api.use('mongo-decimal@0.2.0', 'server');
  api.use('disable-oplog', 'server', { weak: true });

  // If the facts package is loaded, publish some statistics.
  api.use('facts-base', 'server', { weak: true });
  api.addFiles(['init.ts'], 'server');
});
