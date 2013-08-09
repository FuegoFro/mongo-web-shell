/*    Copyright 2013 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/* jshint camelcase: false */
/* global mongo, console */
/**
 * Injects a mongo web shell into the DOM wherever an element of class
 * 'mongo-web-shell' can be found. Additionally sets up the resources
 * required by the web shell, including the mws REST resource, the mws
 * CSS stylesheets, and calls any initialization urls
 */
mongo.init = (function(){
  var loadUrl = function(url, res_id){
    return $.ajax({
      type: 'POST',
      url: url,
      data: JSON.stringify({res_id: res_id}),
      contentType: 'application/json'
    });
  };

  var loadJSON = function(initJson, res_id){
    if (!Object.keys(initJson).length) {
      // no data, save a round-trip
      return $.Deferred().resolve().promise();
    }

    return $.ajax({
      type: 'POST',
      url: '/init/load_json',
      data: JSON.stringify({
        res_id: res_id,
        collections: initJson
      }),
      contentType: 'application/json'
    });
  };

  var loadJSONUrl = function(url, res_id){
    return $.getJSON(url).then(function(data){
      // Condense remote and local JSON
      condenseJson(mongo.init._initState[res_id].initJson, data);
    });
  };

  var condenseJson = function (existingJson, newJson) {
    // Modifies the existing JSON to include the new JSON. The JSON format is
    // {collection_name: [document, document,...], ...}
    $.each(newJson, function (collection, documents) {
      if (!(collection in existingJson)) {
        existingJson[collection] = [];
      }
      $.merge(existingJson[collection], documents);
    });
  };

  var lockShells = function(res_id){
    if (mongo.init._pending[res_id] === undefined) {
      mongo.init._pending[res_id] = 0;
    }
    mongo.init._pending[res_id]++;

    // Lock all affected shells with same res_id
    // Note that this is currently ALL shells since we do not yet assign
    // unique res_ids and all shells share the same res_id
    $.each(mongo.shells, function(i, e){
      e.enableInput(false);
    });
  };

  // unlock shells when all init steps for all shells with res_id are complete
  // can optionally wait for one or more deferred objects to resolve
  // see note above regarding all shells having same res_id
  var unlockShells = function(res_id, waitFor){
    var pending = mongo.init._pending;
    return $.when.apply($, waitFor).then(function(){
      pending[res_id]--;
      if (!pending[res_id]){
        $.each(mongo.shells, function(i, e){
          e.enableInput(true);
        });
      }
    }, function(){
      pending[res_id]--;
      if (!pending[res_id]){
        $.each(mongo.shells, function(i, e){
          e.insertResponseArray([
            'One or more scripts failed during initialization.',
            'Your data may not be completely loaded.  Use the "reset" command to try again.'
          ]);
          e.enableInput(true);
        });
      }
    });
  };

  var initShell = function(shellElement, options){
    options = options || {};

    var $element = $(shellElement);
    if ($element.data('shell') !== undefined) {
      // We have already put a shell in this container, do not re-initialize
      return;
    }

    // Request a resource ID, give it to all the shells, and keep it alive
    mongo.request.createMWSResource(mongo.shells, function (data) {
      var res_id = mongo.init.res_id = data.res_id;
      setInterval(
        function () { mongo.request.keepAlive(data.res_id); },
        mongo.const.keepAliveTime
      );

      var shell = new mongo.Shell(shellElement, mongo.shells.length);
      shell.attachInputHandler(res_id);
      mongo.shells.push(shell);

      if (!mongo.init._initState[res_id]) {
        mongo.init._initState[res_id] = {
          initUrls: [],
          initJson: {},
          initJsonUrls: [],
          shouldInitialize: false
        };
      }
      var initData = mongo.init._initState[res_id];

      // Save init urls
      var initUrl = options.initUrl || $element.data('initialization-url');
      if (initUrl && initData.initUrls.indexOf(initUrl) === -1) {
        initData.initUrls.push(initUrl);
      }

      // Save init JSON/urls
      var jsonAttr = options.initJSON || $element.data('initialization-json');
      if (typeof jsonAttr === 'object'){
        condenseJson(initData.initJson, jsonAttr);
      } else if (jsonAttr && jsonAttr[0] === '{' && jsonAttr[jsonAttr.length - 1] === '}') {
        // If it looks like a JSON object, assume it is supposed to be and try to parse it
        try {
          condenseJson(initData.initJson, JSON.parse(jsonAttr));
        } catch (e) {
          console.error('Unable to parse initialization json: ' + jsonAttr);
        }
      } else if (jsonAttr && initData.initJsonUrls.indexOf(jsonAttr) === -1) {
        // Otherwise assume it's a URL that points to JSON data
        initData.initJsonUrls.push(jsonAttr);
      }

      // If the resource id is new, then we want to run our initializations
      initData.shouldInitialize = initData.shouldInitialize || data.is_new;
    });
  };

  var prepopulateData = function (res_id) {
    // lock shells for init
    lockShells(res_id);

    // First request all remote JSON
    var initData = mongo.init._initState[res_id];
    var remoteJsonRequests = $.map(initData.initJsonUrls, function (url) {
      return loadJSONUrl(url, res_id);
    });

    unlockShells(res_id, remoteJsonRequests).then(function () {
      // Successfully got remote JSON
      initData.initJsonUrls = [];

      lockShells(res_id);

      var waitFor = $.map(initData.initUrls, function (url) {
        return loadUrl(url, res_id);
      });
      waitFor.push(loadJSON(initData.initJson, res_id));

      unlockShells(res_id, waitFor);
    });
  };

  var run = function () {
    mongo.jQueryInit(jQuery);
    mongo.util.enableConsoleProtection();
    var config = mongo.config = mongo.dom.retrieveConfig();
    mongo.dom.injectStylesheet(config.cssPath);

    $(mongo.const.rootElementSelector).mws();
  };

  return {
    run: run,
    prepopulateData: prepopulateData,
    _initState: {},
    _pending: {},
    _lockShells: lockShells,
    _unlockShells: unlockShells,
    _initShell: initShell,
    _loadUrl: loadUrl,
    _loadJSON: loadJSON,
    _loadJSONUrl: loadJSONUrl,
    _jsonCache: {},
    res_id: null
  };
})();
