//Copyright 2012 Google
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

part of '../lawndart.dart';

abstract final class _IDBEventStreamProviders {
  static const EventStreamProvider<Event> successEvent = EventStreamProvider('success');
  static const EventStreamProvider<Event> errorEvent = EventStreamProvider('error');
  static const EventStreamProvider<IDBVersionChangeEvent> blockedEvent = EventStreamProvider('blocked');
  static const EventStreamProvider<IDBVersionChangeEvent> upgradeNeededEvent = EventStreamProvider('upgradeneeded');
  static const EventStreamProvider<Event> completeEvent = EventStreamProvider('complete');
  static const EventStreamProvider<Event> abortEvent = EventStreamProvider('abort');
}

extension IDBRequestEventGetters on IDBRequest? {
  Stream<Event> get onSuccess => _IDBEventStreamProviders.successEvent.forTarget(this);
  Stream<Event> get onError => _IDBEventStreamProviders.errorEvent.forTarget(this);
  Stream<IDBVersionChangeEvent> get onBlocked => _IDBEventStreamProviders.blockedEvent.forTarget(this);
  Stream<IDBVersionChangeEvent> get onUpgradeNeeded => _IDBEventStreamProviders.upgradeNeededEvent.forTarget(this);
  Stream<Event> get onAbort => _IDBEventStreamProviders.abortEvent.forTarget(this);

  Future<T> toDartFuture<T>() {
    if (this == null) {
      return Future.value(null);
    }

    final completer = Completer<T>.sync();

    onSuccess.first.then((evt) => completer.complete(this!.result as T));
    onError.first.then((evt) => completer.completeError(this!.error!));

    return completer.future;
  }
}

class AbortedException implements Exception {
  AbortedException();

  @override
  String toString() => "AbortedException: Transaction aborted";
}

extension IDBTransactionEventGetters on IDBTransaction {
  Stream<Event> get onComplete => _IDBEventStreamProviders.completeEvent.forTarget(this);
  Stream<Event> get onAbort => _IDBEventStreamProviders.abortEvent.forTarget(this);
  Stream<Event> get onError => _IDBEventStreamProviders.errorEvent.forTarget(this);

  Future<void> get completed {
    final completer = Completer<void>.sync();

    onComplete.first.then((_) => completer.complete());

    onError.first.then((_) {
      if (completer.isCompleted) return;

      if (error != null) {
        completer.completeError(error!);
      } else {
        completer.completeError(Exception('unknown IndexedDB transaction error'));
      }
    });

    onAbort.first.then((_) {
      if (completer.isCompleted) return;

      completer.completeError(AbortedException());
    });

    return completer.future;
  }
}

/**
 * Wraps the IndexedDB API and exposes it as a [Store].
 * IndexedDB is generally the preferred API if it is available.
 */
class IndexedDbStore extends Store {
  static Map<String, IDBDatabase> _databases = new Map<String, IDBDatabase>();

  final String dbName;
  final String storeName;

  IndexedDbStore._(this.dbName, this.storeName) : super._();

  static Future<IndexedDbStore> open(String dbName, String storeName) async {
    var store = new IndexedDbStore._(dbName, storeName);
    await store._open();
    return store;
  }

  /// Returns true if IndexedDB is supported on this platform.
  static bool get supported => window.has('indexedDB');

  @override
  void close() => _db?.close();

  Future _open() async {
    if (!supported) {
      throw new UnsupportedError('IndexedDB is not supported on this platform');
    }

    _db?.close();

    var db = (await window.indexedDB.open(dbName).toDartFuture<IDBDatabase?>())!;
    if (!db.objectStoreNames.contains(storeName)) {
      db.close();
      db = (await (window.indexedDB.open(dbName, db.version + 1)
            ..onUpgradeNeeded.first.then((e) {
              final d = (e.target! as IDBOpenDBRequest).result! as IDBDatabase;
              d.createObjectStore(storeName);
            }))
          .toDartFuture<IDBDatabase?>())!;
    }

    _databases[dbName] = db;
    return true;
  }

  IDBDatabase? get _db => _databases[dbName];

  @override
  Future<void> removeByKey(String key) =>
    _runInTxn((store) => store.delete(key.toJS).toDartFuture<void>());

  @override
  Future<String> save(String obj, String key) =>
    _runInTxn((store) =>
      store.put(obj.toJS, key.toJS).toDartFuture<JSString>()
        .then((str) => str.toDart));

  @override
  Future<String?> getByKey(String key) =>
    _runInTxn((store) =>
      store.get(key.toJS).toDartFuture<JSString?>()
        .then((str) => str?.toDart),
      'readonly');

  @override
  Future<void> nuke() =>
    _runInTxn((store) => store.clear().toDartFuture<void>());

  Future<T> _runInTxn<T>(Future<T> requestCommand(IDBObjectStore store), [String txnMode = 'readwrite']) async {
    var trans = _db!.transaction(storeName.toJS, txnMode);
    final completed = trans.completed;

    var store = trans.objectStore(storeName);
    var result = requestCommand(store);
    await completed;
    return result;
  }

  Stream<String> _doGetAll(String onCursor(IDBCursorWithValue cursor)) {
    var trans = _db!.transaction(storeName.toJS, 'readonly');
    var store = trans.objectStore(storeName);
    final req = store.openCursor(null, 'next');

    return req.onSuccess
      .map((evt) => (evt.target as IDBRequest?)?.result as IDBCursorWithValue?)
      .takeWhile((cursor) => cursor != null)
      .cast<IDBCursorWithValue>()
      .map((cursor) {
        final result = onCursor(cursor);
        cursor.continue_();
        return result;
      });
  }

  @override
  Stream<String> all() =>
    _doGetAll((cursor) => (cursor.value! as JSString).toDart);

  @override
  Future<void> batch(Map<String, String> objs) =>
    _runInTxn((store) =>
      Future.wait(
        objs.entries.map((entry) =>
          store.put(entry.value.toJS, entry.key.toJS).toDartFuture()
    )));

  @override
  Stream<String> getByKeys(Iterable<String> keys) async* {
    for (var key in keys) {
      var v = await getByKey(key);
      if (v != null) yield v;
    }
  }

  @override
  Future<void> removeByKeys(Iterable<String> keys) async =>
    await _runInTxn((store) =>
      Future.wait(keys.map((key) =>
        store.delete(key.toJS).toDartFuture()
    )));

  @override
  Future<bool> exists(String key) async =>
    (await getByKey(key)) != null;

  @override
  Stream<String> keys() =>
    _doGetAll((cursor) => (cursor.key! as JSString).toDart);
}
