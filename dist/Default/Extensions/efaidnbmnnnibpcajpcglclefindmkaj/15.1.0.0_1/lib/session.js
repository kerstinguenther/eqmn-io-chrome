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

/*global chrome, console, require:true, exports:true, define:true, $, def:true, SETTINGS */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("util"),
        require("common"),
        require("proxy"),
        require("communicate"),
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
    util,
    common,
    proxy,
    communicate,
    analytics
) {
    'use strict';
    var singleton = null, prop, upload, zip, maxSize;

    upload = function () {
        return communicate.getModule("upload");
    };

    zip = function () {
        return communicate.getModule("convert-to-zip");
    };

    function Session() {
        var sign_in_promise, sessionTab, sessionRequest;

        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        this.updateName = function (assetid, name) {
            if (!assetid || !name) {
                return util.Deferred().resolve().promise();
            }
            var url = common.settings.files_api +
                        "assets/" +
                        assetid +
                        "/metadata/name",
                data = JSON.stringify(
                    {
                        value: name,
                        on_dup_name: "auto_rename"
                    }
                );
            return util.ajax(
                {
                    type: "PUT",
                    url: url,
                    data: data,
                    headers: common.POST_headers()
                }
            ).then(
                function () {},
                this.proxy(function (xhr) {
                    proxy.REST_error(xhr, this, {url: url, data: data});
                })
            );
        };

        this.updateParent = function (assetid, parentid) {
            if (!assetid || !parentid) {
                return util.Deferred().resolve().promise();
            }
            var url = common.settings.files_api +
                    "assets/" +
                    sessionRequest.assetId +
                    "/metadata/parent_id",
                data = JSON.stringify(
                    {
                        value: parentid,
                        on_dup_name: "auto_rename"
                    }
                );
            return util.ajax(
                {
                    type: "PUT",
                    url: url,
                    data: data,
                    headers: common.POST_headers()
                }
            ).then(
                function () {},
                this.proxy(function (xhr) {
                    proxy.REST_error(xhr, this, {url: url, data: data});
                })
            );
        };

        this.rename = function (request) {
            var renamePromise = util.Deferred();
            this.updateName(request.assetId, request.filename).then(
                this.proxy(
                    function () {
                        this.updateParent(
                            request.assetId,
                            request.dest_folder
                        ).then(
                            function () {
                                renamePromise.resolve();
                            },
                            function () {
                                renamePromise.reject();
                            }
                        );
                    }
                ),
                function () {
                    renamePromise.reject();
                }
            );

            return renamePromise.promise();
        };

        this.gotoPath = function (path) {
            var ready = util.Deferred();
            if (sessionRequest.userSelectPromise) {
                sessionRequest.userSelectPromise.then(
                    function () {ready.resolve(); },
                    function () {ready.reject(); }
                );
            } else {
                ready.resolve();
            }

            ready.then(
                this.proxy(function () {
                    common.sso_url(path).then(
                        this.proxy(function (sso) {
                            if (sessionTab) {
                                if (util.isChrome()) {
                                    chrome.tabs.update(
                                        sessionTab,
                                        {url: sso.uri, active: true}
                                    );
                                }
                                sessionTab = null;
                            } else {
                                util.createTab(sso.uri);
                            }
                        }),
                        this.proxy(function (sso) {
                            // continue without SSO
                            path = common.settings.cloud_host + path;
                            if (sessionTab) {
                                if (util.isChrome()) {
                                    chrome.tabs.update(
                                        sessionTab,
                                        {url: path, active: true}
                                    );
                                }
                                sessionTab = null;
                            } else {
                                util.createTab(path);
                            }
                        })
                    );
                })
            );
        };

        this.file_spec_done = function (details) {
            sessionRequest.dest_folder = details.dest_folder;
            var name = sessionRequest.filename;
            if (name.substring(name.length - 3, name.length) !== "zip") {
                sessionRequest.filename = details.filename;
            }
            // don't rename until upload complete...

            // Natia: the user will give us a filename.pdf.
            // But in the HTML case, we want to rename to filename.zip
            // the convert to PDF will create filename.pdf
            sessionRequest.upload_promise.then(
                this.proxy(function () {
                    this.rename(sessionRequest).then(
                        function () {
                            sessionRequest.userSelectPromise.resolve();
                        },
                        function () {
                            sessionRequest.userSelectPromise.reject();
                        }
                    );
                })
            );
        };

        this.message = function (msg, busy, error) {
            if (sessionTab) {
                util.sendMessage(
                    {
                        tabId       : sessionTab,
                        progress_op : error ? "set-error" : "set-text",
                        text        : msg,
                        busy        : busy
                    }
                );
            }
        };

        this.foundCode = function (url) {
            var code = url.split("?")[1].split("=")[1].split("&")[0];

            this.newSession(null, true);
            analytics.event(analytics.e.SIGN_IN_COMPLETE);
            common.authorize(code).then(
                function () {
                    if (sign_in_promise) {
                        sign_in_promise.resolve();
                    }
                    sign_in_promise = null;
                },
                function () {
                    if (sign_in_promise) {
                        sign_in_promise.reject();
                    }
                    sign_in_promise = null;
                }
            );
        };

        this.sessionRequest = function (request, params) {
            sessionRequest = request;
            sessionRequest.params =
                params || {filename: sessionRequest.filename};
        };

        this.send_folders = function () {
            this.signed_in().then(this.proxy(function () {
                analytics.event(analytics.e.UPLOAD_RENAME_CLICKED);
                util.ajax(
                    {
                        type: "POST",
                        headers: common.POST_headers(),
                        url: common.settings.files_api + "search",
                        data: JSON.stringify(
                            {
                                "q": {"object_type": "folder"},
                                "metadata": [
                                    "id",
                                    "name",
                                    "created",
                                    "parent_id"
                                ]
                            }
                        )
                    }

                ).then(
                    this.proxy(function (data) {
                        if (sessionTab) {
                            // add the root folder to the result
                            data.results.push({
                                "parent_id": null,
                                "object_type": "root",
                                "created": "2015-03-31T11:37:40",
                                "id": common.settings.files_root,
                                "name": "/"
                            });
                            util.sendMessage(
                                {
                                    tabId       : sessionTab,
                                    progress_op : "folders",
                                    folders     : data.results
                                }
                            );
                        }
                    }),
                    this.proxy(function (xhr, status, err) {
                        proxy.REST_error(xhr, this);
                    })
                );
            }));
        };


        this.newSession = function (url, keep, params) {
            if (!params) {
                params  = sessionRequest.params;
            }
            if (params.filename) {
                sessionRequest.userSelectPromise = util.Deferred();
            }
            if (!url) {
                if (util.isChrome()) {
                    url = chrome.runtime.getURL("data/js/progress.html") +
                            "#" + util.param(params);
                }
                if (params.filename) {
                    this.send_folders();
                }
            }

            if (sessionTab) {
                if (util.isChrome()) {
                    chrome.tabs.update(sessionTab, {url: url, active: true});
                }

            } else {
                if (util.isChrome()) {
                    chrome.tabs.create(
                        {
                            url: url
                        },
                        this.proxy(function (tab) {
                            if (keep) {
                                sessionTab = tab.id;
                            }
                        })
                    );
                }
                if (util.isFF()) {
                    require("sdk/windows").browserWindows.open({url: url});
                }
            }
        };

        this.sign_in = function (request, sender) {
            if (sign_in_promise) {
                sign_in_promise.reject();
                sign_in_promise = null;
                analytics.event(analytics.e.SIGN_IN_ABANDONED);
            }

            sign_in_promise = util.Deferred();
            common.baseUris().then(this.proxy(function () {
                var params = {
                    client_id: common.settings.ims_client_id,
                    redirect_uri: common.settings.redirect_uri,
                    scope: "AdobeID, skybox, openid",
                    dc: false
                },
                    url = common.settings.ims_host +
                    "ims/authorize/v1?" +
                    util.param(params);
                analytics.event(analytics.e.SIGN_IN_SHOWN);
                this.newSession(url, true);
            }));
            return sign_in_promise.promise();
        };

        this.signed_in = function () {
            if (sign_in_promise) {
                return sign_in_promise;
            }
            return util.Deferred().resolve().promise();
        };

        this.signOut = function () {
            if (util.isChrome()) {
                chrome.tabs.create({
                    url: common.settings.ims_host +
                            "ims/logout/v1?" +
                            util.param(
                            {
                                access_token:
                                    common.settings.auth_token.replace(
                                        /Bearer /,
                                        ""
                                    ),
                                client_id: common.settings.ims_client_id,
                                client_secret:
                                    common.settings.ims_client_secret,
                                redirect_uri: "https://adobe.com"
                            }
                        )
                });
            }
            common.clearAuth();
        };

        this.checkSessionTab = function (tabId, url) {
            if (tabId === sessionTab &&
                    url.indexOf("chrome-extension:") === -1 &&
                    url.indexOf(".adobe.com/") === -1) {
                sessionTab = null;
                if (sign_in_promise) {
                    sign_in_promise.reject();
                    sign_in_promise = null;
                    analytics.event(analytics.e.SIGN_IN_ABANDONED);
                }
            }
        };

        this.goto_acom = function (request) {
            if (SETTINGS.USE_ACROBAT) {
                util.createTab(common.settings.cloud_host);
            } else {
                this.sessionRequest(request, {});
                this.gotoPath("files");
            }
        };

        this.html_to_pdf = function (request, sender) {
            request.filename = sender.tab.title
                .replace(/[\*"\/\\'&\.]/g, "") + ".pdf";

            this.sessionRequest(
                request,
                {
                    filename: request.filename,
                    message: "Gathering HTML Content",
                    busy: "true",
                    progress_op: "htmlToPdf"
                }
            );
            this.sign_in(request, sender).then(this.proxy(function () {
                if (util.isChrome()) {
                    chrome.tabs.executeScript(
                        request.tabId,
                        {
                            code: "var maxSize= " + SETTINGS.MAX_HTML_SIZE +
                                ", DEBUG = " + SETTINGS.DEBUG_MODE +
                                ", TABID = " + request.tabId +
                                ", OP = 'html-blob'" +
                                ", EXCLUDE = [];",
                            allFrames: true
                        },
                        function () {
                            chrome.tabs.executeScript(
                                request.tabId,
                                {
                                    file: "data/js/get-html.js",
                                    allFrames: true
                                }
                            );
                        }
                    );
                }

            }));
        };

        this.relay_message = function (request) {
            if (request.complete) {
				if (request.error) {
					util.consoleLog(request.error);
					analytics.event(
                        analytics.e.HTML_SOURCE_SIZE_TOO_LARGE_ERROR
                    );
				} else {
					request.html_op = "serialize_iframe";
					if (util.isChrome()) {
						chrome.tabs.executeScript(
							request.tabId,
							{
                                // some defensive code to make
                                // sure that we don't throw errors at
                                // the client.
                                // (Can happen in iframes that are
                                // changing state)
								code: "if (window.dc && window.OP) {" +
                                        " receiveIframe(" +
										JSON.stringify(request) +
										"); }",
								allFrames: true
							}
						);
					}
				}
            } else {
                if (util.isChrome()) {
                    chrome.tabs.executeScript(
                        request.tabId,
                        {
                            code: "if (window.dc && window.OP) {serialize(" +
                                    JSON.stringify(request) +
                                    "); }",
                            allFrames: true
                        }
                    );
                }
            }
        };

        this.flickr = function (request) {
            this.sessionRequest(request, {});
            this.newSession(null, false, {unavailable: "flickr"});
        };

        this.error = function (params) {
            if (sessionTab) {
                if (sessionRequest) {
                    if (sessionRequest.userSelectPromise) {
                        sessionRequest.userSelectPromise.reject();
                    }
                }

                var message = ["Please Report this Error:"];
                util.each(params, function (name, value) {
                    message.push(name + ": " + value);
                });
                this.message(message.join("\n"), false, true);
                sessionTab = null;
            }
        };

        proxy.handlers(this.error.bind(this));
        if (util.isChrome()) {
            chrome.tabs.onRemoved.addListener(function (tabId) {
                if (tabId === sessionTab) {
                    sessionTab = null;
                    if (sign_in_promise) {
                        analytics.event(analytics.e.SIGN_IN_ABANDONED);
                        sign_in_promise.reject();
                        sign_in_promise = null;
                    }
                }
            });
        }

        this.html_blob = function (request) {
            //there was an error creating html dom
            if (request.error) {
                util.consoleError(request.error);
                analytics.logError(request.error, request.analytics[0]);
            } else {
                analytics.logContents(request);

                //analytics for blob size (size in MB)
                analytics.checkAndLogHTMLBlobSize(
                    request.blob.currentSize / 1048576
                );
                //log analytics for html content
                if (request.analytics) {
                    analytics.setParamsAndLogAnalytics(
                        request.analytics,
                        analytics.e.HTML_SOURCE_CONTENT,
                        "content"
                    );
                }
                //log analytics for dom clone timing
                analytics.setArg("stage", "CLONE");
                analytics.checkAndLogTimingRange(request.cloneTiming);

                util.consoleLog("html blob send to:" + sessionTab);
                //proceed with compression
                var blob_text = JSON.stringify(request.blob);
                zip().compress(blob_text, sessionRequest);
            }
        };

    }

    if (!singleton) {
        singleton = new Session();
        communicate.registerHandlers(
            {
                "goto_acom"         : singleton.proxy(singleton.goto_acom),
                "html_to_pdf"       : singleton.proxy(singleton.html_to_pdf),
                "flickr"            : singleton.proxy(singleton.flickr),
                "file_spec_done"    : singleton.proxy(singleton.file_spec_done),
                "html-blob"         : singleton.proxy(singleton.html_blob),
                "relay-msg"         : singleton.proxy(singleton.relay_message)
            }
        );
        communicate.registerModule("session", singleton);
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
