export const listenAll = async function (cursorDescription, listenCallback) {
  const listeners = [];
  await forEachTrigger(cursorDescription, function (trigger) {
    listeners.push(DDPServer._InvalidationCrossbar.listen(trigger, listenCallback));
  });

  return {
    stop: function () {
      listeners.forEach(function (listener) {
        listener.stop();
      });
    },
  };
};

export const forEachTrigger = async function (cursorDescription, triggerCallback) {
  const key = { collection: cursorDescription.collectionName };
  const specificIds = LocalCollection._idsMatchedBySelector(cursorDescription.selector);
  if (specificIds) {
    for (const id of specificIds) {
      await triggerCallback(Object.assign({ id: id }, key));
    }
    await triggerCallback(Object.assign({ dropCollection: true, id: null }, key));
  } else {
    await triggerCallback(key);
  }
  // Everyone cares about the database being dropped.
  await triggerCallback({ dropDatabase: true });
};
