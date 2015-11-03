/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/

/*global chrome, console, require:true, exports:true, define:true, $, def:true */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("communicate"),
        require("util"),
        require("common"),
        require("proxy"),
        require("analytics")
    ];
}

var def;
require = function (m) {'use strict'; return m; };
if (window.define) {
    def = window.define;
} else {
    def = function (deps, fn) {
        'use strict';
        return fn.apply(null, [{
            ajax: $.ajax.bind($)
        }]);
    };
}
var exports = acom_analytics = {};

def(dependOn(), function (
    communicate,
    util,
    common,
    proxy,
    analytics
) {
    'use strict';
    var singleton = null, Upload, prop, session;

    session = function () {
        return communicate.getModule("session");
    };

    function filenameFromUrl(url) {
        return url
                .replace(/\/$/, "")
                .split("/")
                .splice(-1)[0]
                .replace(/\?\S*/, "");
    }

    Upload = function () {
        this.LOG = common.LOG;
        this.proxy = proxy.proxy.bind(this);

        this.ajaxRequest = function (request) {
            var ajaxPromise = util.Deferred();
            common.ajaxReady(true).then(
                function () {
                    session().newSession(
                        null,
                        true,
                        {
                            filename: request.filename
                        }
                    );
                    ajaxPromise.resolve();
                },
                this.proxy(function () {
                    session().sign_in(request).then(
                        function () {
                            ajaxPromise.resolve();
                        },
                        function () {
                            ajaxPromise.reject();
                            util.consoleLog("upload rejected");
                        }
                    );
                })
            );
            return ajaxPromise;
        };
        
        this.sendIt = function (blob, request) {
            var fd = util.newFormData(), metadata;

            metadata = {
                name: request.filename,
                parent_id: common.settings.files_root,
                on_dup_name: "auto_rename",
                ignore_content_type: true,
                source_url: request.url
            };

            fd.append("metadata", JSON.stringify(metadata));
            fd.append("file", blob, request.filename);

            return util.ajax({
                url: common.settings.files_upload + "assets",
                type: "POST",
                processData: false,
                data: fd,
                contentType: false,
                headers: common.GET_headers()
            }).then(
                function (data) {
                    data.request = request;
                    return data;
                },
                this.proxy(function (xhr) {
                    proxy.REST_error(xhr, this);
                    return xhr;
                })
            );
        };

        this.upload = function (request) {
            request.upload_promise = util.Deferred();
            if (!request.filename) {
                request.filename = filenameFromUrl(request.url);
            }
            session().sessionRequest(request);
            this.ajaxRequest(request).then(
                this.proxy(function () {
                    analytics.event(analytics.e.UPLOAD_PROGRESS_SHOWN);

                    // get the file contents locally
                    var blob,
                        xhr;

                    if (request.url.indexOf("data") === 0) {
                        throw new Error("Can't upload Data URIs (Yet)");
                    }
                    xhr = util.newXHR();
                    xhr.open('GET',  request.url, true);
                    xhr.responseType = 'blob';
                    xhr.onload = this.proxy(function (e) {

                        if (xhr.status === 200) {

                            if (request.mime &&
                                    xhr.response.type !== request.mime) {
                                session().error(
                                    {
                                        error: "Unexpected mime type",
                                        received: xhr.response.type,
                                        expected: request.mime,
                                        url: request.url,
                                        timestamp: (new Date()).getTime()
                                    }
                                );
                                this.LOG("UnexpectedMimeType: " +
                                         xhr.response.type +
                                         " ExpectedMimeType" +
                                         request.mime
                                        );
                                analytics.event(
                                    analytics.e.ERROR_WRONG_MIME_TYPE
                                );
                                request.upload_promise.reject(xhr);
                            }

                            var blob = util.newBlob([xhr.response]);

                            this.sendIt(blob, request).then(
                                this.proxy(function (data) {
                                    util.consoleLogDir(data);
                                    request.upload_promise.resolve(data);
                                    return data;
                                }),
                                this.proxy(function (xhr) {
                                    request.upload_promise.reject(xhr);
                                    proxy.REST_error(xhr, this);
                                    return xhr;
                                })
                            );
                        }
                    });
                    xhr.send();
                })
            );
            return request.upload_promise.promise();
        };

        this.uploadHTML = function (blob, request) {
            request.upload_promise = util.Deferred();
            if (!request.filename) {
                request.filename = filenameFromUrl(request.url);
            }
            //rename to zip before uploading it to cloud
            request.filename = request.filename.replace(".pdf", ".zip");
            this.sendIt(blob, request).then(
                this.proxy(function (data) {
                    util.consoleLogDir(data);
                    request.upload_promise.resolve(data);
                    return data;
                })
            );
            return request.upload_promise.promise();
        };
    };
    
    if (!singleton) {
        singleton = new Upload();
        communicate.registerModule("upload", singleton);
    }
    for (prop in singleton) {
        if (singleton.hasOwnProperty(prop)) {
            if (typeof (singleton[prop]) === "function") {
                exports[prop] = singleton[prop].bind(singleton);
            } else {
                exports[prop] = singleton[prop];
            }
        }
    }

    return singleton;
});
