import { MongoInternals } from 'meteor/mongo';
import { Cursor } from './cursor';
import { CursorDescription } from './cursor_description';
import { NewOplogObserveDriver } from './new_oplog_observe_driver.ts';
import { ObserveHandle } from './observe_handle.ts';
import { ObserveMultiplexer } from './observe_multiplex.ts';
import { PollingObserveDriver } from './polling_observe_driver.ts';
import { NewOplogTailing } from './new_oplog_tailing.ts';
const oplogCollectionWarnings: string[] = [];

MongoInternals.Connection.prototype._observeChanges = async function (
  cursorDescription,
  ordered,
  callbacks,
  nonMutatingCallbacks
) {
  var self = this;

  if (process.env.MONGO_OPLOG_URL && !self._oplogHandle) {
    self._oplogHandle = new NewOplogTailing(
      process.env.MONGO_OPLOG_URL,
      self.db.databaseName
    );
  }

  const collectionName = cursorDescription.collectionName;

  if (cursorDescription.options.tailable) {
    return self._observeChangesTailable(cursorDescription, ordered, callbacks);
  }

  // You may not filter out _id when observing changes, because the id is a core
  // part of the observeChanges API.
  const fieldsOptions =
    cursorDescription.options.projection || cursorDescription.options.fields;
  if (fieldsOptions && (fieldsOptions._id === 0 || fieldsOptions._id === false)) {
    throw Error('You may not observe a cursor with {fields: {_id: 0}}');
  }

  var observeKey = EJSON.stringify(
    Object.assign({ ordered: ordered }, cursorDescription)
  );

  var multiplexer, observeDriver;
  var firstHandle = false;

  // Find a matching ObserveMultiplexer, or create a new one. This next block is
  // guaranteed to not yield (and it doesn't call anything that can observe a
  // new query), so no other calls to this function can interleave with it.
  if (observeKey in self._observeMultiplexers) {
    multiplexer = self._observeMultiplexers[observeKey];
  } else {
    firstHandle = true;
    // Create a new ObserveMultiplexer.
    multiplexer = new ObserveMultiplexer({
      ordered: ordered,
      onStop: function () {
        delete self._observeMultiplexers[observeKey];
        return observeDriver.stop();
      },
    });
  }

  var observeHandle = new ObserveHandle(multiplexer, callbacks, nonMutatingCallbacks);

  const oplogOptions = self?._oplogHandle?._oplogOptions || {};
  const { includeCollections, excludeCollections } = oplogOptions;
  if (firstHandle) {
    var matcher, sorter;
    var canUseOplog = [
      function () {
        // At a bare minimum, using the oplog requires us to have an oplog, to
        // want unordered callbacks, and to not want a callback on the polls
        // that won't happen.
        return self._oplogHandle && !ordered && !callbacks._testOnlyPollCallback;
      },
      function () {
        // We also need to check, if the collection of this Cursor is actually being "watched" by the Oplog handle
        // if not, we have to fallback to long polling
        if (excludeCollections?.length && excludeCollections.includes(collectionName)) {
          if (!oplogCollectionWarnings.includes(collectionName)) {
            console.warn(
              `Meteor.settings.packages.mongo.oplogExcludeCollections includes the collection ${collectionName} - your subscriptions will only use long polling!`
            );
            oplogCollectionWarnings.push(collectionName); // we only want to show the warnings once per collection!
          }
          return false;
        }
        if (includeCollections?.length && !includeCollections.includes(collectionName)) {
          if (!oplogCollectionWarnings.includes(collectionName)) {
            console.warn(
              `Meteor.settings.packages.mongo.oplogIncludeCollections does not include the collection ${collectionName} - your subscriptions will only use long polling!`
            );
            oplogCollectionWarnings.push(collectionName); // we only want to show the warnings once per collection!
          }
          return false;
        }
        return true;
      },
      function () {
        // We need to be able to compile the selector. Fall back to polling for
        // some newfangled $selector that minimongo doesn't support yet.
        try {
          matcher = new Minimongo.Matcher(cursorDescription.selector);
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      },
      function () {
        // ... and the selector itself needs to support oplog.
        return NewOplogObserveDriver.cursorSupported(cursorDescription, matcher);
      },
      function () {
        // And we need to be able to compile the sort, if any.  eg, can't be
        // {$natural: 1}.
        if (!cursorDescription.options.sort) return true;
        try {
          sorter = new Minimongo.Sorter(cursorDescription.options.sort);
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      },
    ].every((f) => f()); // invoke each function and check if all return true

    let driverClass;
    if (canUseOplog) {
      driverClass = NewOplogObserveDriver;
    } else {
      driverClass = PollingObserveDriver;
    }

    observeDriver = new driverClass({
      cursorDescription: cursorDescription,
      mongoHandle: self,
      multiplexer: multiplexer,
      ordered: ordered,
      matcher: matcher, // ignored by polling
      sorter: sorter, // ignored by polling
      _testOnlyPollCallback: callbacks._testOnlyPollCallback,
    });

    if (observeDriver._init) {
      await observeDriver._init();
    }

    // This field is only set for use in tests.
    multiplexer._observeDriver = observeDriver;
  }
  self._observeMultiplexers[observeKey] = multiplexer;
  // Blocks until the initial adds have been sent.
  await multiplexer.addHandleAndSendInitialAdds(observeHandle);

  return observeHandle;
};

MongoInternals.Connection.prototype.find = function (collectionName, selector, options) {
  var self = this;

  if (arguments.length === 1) selector = {};

  return new Cursor(self, new CursorDescription(collectionName, selector, options));
};
