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

/**
 * Wraps the local storage API and exposes it as a [Store].
 * Local storage is a synchronous API, and generally not recommended
 * unless all other storage mechanisms are unavailable.
 */
class LocalStorageStore extends Store {
  LocalStorageStore._() : super._();

  static List<String> _allKeys() {
    final result = <String>[];
    for (var i = 0; i < window.localStorage.length; i++) {
      result.add(window.localStorage.key(i)!);
    }
    return result;
  }

  static List<String> _allValues() {
    final result = <String>[];
    for (var i = 0; i < window.localStorage.length; i++) {
      result.add(window.localStorage.getItem(window.localStorage.key(i)!)!);
    }
    return result;
  }

  static Future<LocalStorageStore> open() async {
    var store = new LocalStorageStore._();
    await store._open();
    return store;
  }

  @override
  Future<void> _open() async {}

  @override
  Stream<String> keys() => Stream.fromIterable(LocalStorageStore._allKeys());

  @override
  Stream<String> all() => Stream.fromIterable(LocalStorageStore._allValues());

  @override
  Future<bool> exists(String key) async => window.localStorage.getItem(key) != null;

  @override
  Future<String?> getByKey(String key) async => window.localStorage.getItem(key);

  @override
  Future<void> removeByKey(String key) async => window.localStorage.removeItem(key);

  @override
  Stream<String> getByKeys(Iterable<String> keys) =>
    Stream.fromIterable(
      keys
        .map((key) => window.localStorage.getItem(key))
        .whereType()
    );

  @override
  Future<void> removeByKeys(Iterable<String> keys) async =>
    keys.forEach((key) => window.localStorage.removeItem(key));

  @override
  Future<void> batch(Map<String, String> objectsByKey) async =>
    objectsByKey.forEach((key, value) => window.localStorage.setItem(key, value));

  @override
  Future<String> save(String obj, String key) async {
    window.localStorage.setItem(key, obj);
    return key;
  }

  @override
  Future<void> nuke() async => window.localStorage.clear();
}
