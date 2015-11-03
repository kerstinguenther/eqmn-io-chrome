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

/*global console, def:true, require:true, exports:true, define:true, chrome, SETTINGS */
/*jslint devel: true, browser: true, nomen: true, maxlen: 90 */

function dependOn() {
    'use strict';
    return [
        require("util"),
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

def(dependOn(), function (util, proxy, analytics) {
    'use strict';
    var singleton = null, prop,
        template = "/static/js/aicuc/cpdf-template/sign_in_complete.html",
        servers = {
            "local-dev": {
                cloud_host: "http://local-test.acrobat.com:8002/",
                ims_client_id: "acom_extension_test1",
                ims_client_secret: "2a9d3f64-2381-4f68-ba1a-0dcdd4525a28",
                redirect_uri: "http://createpdf.dev.dexilab.acrobat.com" + template,
                cpdf_host: "http://local-test.acrobat.com:8000/",
                env: "local-dev"
            },

            dev: {
                cloud_host: "http://cloud.dev.dexilab.acrobat.com/",
                ims_client_id: "acom_extension",
                ims_client_secret: "7f5cc40b-441c-4a4a-85ac-5b8037a67a7c",
                redirect_uri: "https://createpdf.stage.acrobat.com" + template,
                cpdf_host: "http://createpdf.dev.dexilab.acrobat.com/",
                env: "dev"
            },
            test: {
                cloud_host: "https://cloud.test.dexilab.acrobat.com/",
                ims_client_id: "acom_extension",
                ims_client_secret: "7f5cc40b-441c-4a4a-85ac-5b8037a67a7c",
                redirect_uri: "https://createpdf.stage.acrobat.com" + template,
                cpdf_host: "https://createpdf.test.dexilab.acrobat.com/",
                env: "test"
            },
            stage: {
                cloud_host: "https://cloud.stage.acrobat.com/",
                ims_client_id: "acom_extension",
                ims_client_secret: "7f5cc40b-441c-4a4a-85ac-5b8037a67a7c",
                redirect_uri: "https://createpdf.stage.acrobat.com" + template,
                cpdf_host: "https://createpdf.stage.acrobat.com/",
                env: "stage"
            },

            prod: {
                cloud_host: "https://cloud.acrobat.com/",
                ims_client_id: "acom_extension",
                ims_client_secret: "f8f770cb-748d-441c-97dc-d9d684ab6698",
                redirect_uri: "https://createpdf.acrobat.com" + template,
                cpdf_host: "https://createpdf.acrobat.com/",
                env: "prod"
            }
        },
        server,

        uninstall = function () {
            analytics.event(analytics.e.EXTENSION_FORCE_UNINSTALL);
            if (util.isFF()) {
                var Cu = require("chrome").Cu,
                    AddonManager = Cu["import"]("resource://gre/modules/AddonManager.jsm")
                        .AddonManager,
                    id = require("sdk/self").id;

                AddonManager.getAddonByID(id, function (addon) {addon.uninstall(); });
            } else {
                chrome.management.uninstallSelf();
            }
        },

        showMessage = function (msg) {

            if (util.isFF()) {
                var chrome = require("chrome"),
                    Cc = chrome.Cc,
                    Ci = chrome.Ci,
                    prompts = Cc["@mozilla.org/embedcomp/prompt-service;1"]
                        .getService(Ci.nsIPromptService);

                prompts.alert(null, "", msg);
            } else {
                alert(msg);
            }
            return;
        },

        Common = function () {
            var actionButton,
                $GET_headers = {
                    Accept: "application/vnd.adobe.dex+json;version=1",
                    "Authorization": null,
                    "x-api-client-id": "api_browser_ext"
                },
                $POST_headers = {
                    Accept: "application/vnd.adobe.dex+json;version=1",
                    "Content-Type":
                        "application/vnd.adobe.dex+json;version=1;charset=utf-8",
                    "Authorization": null,
                    "x-api-client-id": "api_browser_ext"
                },
                uuid = function () {
                    return "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                        .replace(
                            /x/g,
                            function () {
                                return '0123456789abcdef'[Math.floor(Math.random() * 16)];
                            }
                        );
                };

            this.proxy = proxy.proxy.bind(this);
            this.util = util;

            this.reset = function (srvr) {
                this.settings = {
                    cpdf_api: null,
                    files_host: null,
                    files_api: null,
                    files_upload: null,
                    files_root: null,
                    fillsign_api:  null,
                    auth_token: null,
                    ims_host: null
                };
                // set the applicable server here...
                util.isDevEnv().then(this.proxy(function (devEnv) {
                    if (typeof (srvr) === "undefined") {
                        srvr = devEnv ? "test" : "prod";
                    }
                    server = servers[srvr];
                    util.extend(this.settings, server);
                }));
            };
            this.reset(server);

            this.connected = function () {
                return !!this.settings.auth_token;
            };

            this.setGlobals = function (globals) {
                this.globals = globals;
            };

            this.GET_headers = function () {
                $GET_headers["x-request-id"] = uuid();
                $GET_headers.Authorization = this.settings.auth_token;
                return $GET_headers;
            };

            this.POST_headers = function () {
                $POST_headers["x-request-id"] = uuid();
                $POST_headers.Authorization = this.settings.auth_token;
                return $POST_headers;
            };

            this.noToken = function (p) {
                if (p) {
                    p.reject();
                }
            };

            this.filesBaseUris = function () {
                var baseUrisPromise = util.Deferred();
                if (this.settings.files_api) {
                    baseUrisPromise.resolve();

                } else {
                    util.ajax(
                        {
                            url: this.settings.files_host + "api/base_uris",
                            headers: {
                                Accept: this.GET_headers().Accept,
                                "x-api-client-id": this.GET_headers()["x-api-client-id"]
                            }
                        }
                    ).then(
                        this.proxy(function (fileUris) {
                            this.settings.files_api = fileUris.api;
                            this.settings.files_upload = fileUris.upload;
                            baseUrisPromise.resolve();
                        }),
                        function () {
                            baseUrisPromise.reject();
                        }
                    );
                }
                return baseUrisPromise.promise();
            };

            this.cloudBaseUris = function () {
                var baseUrisPromise = util.Deferred();
                if (this.settings.cloud_api) {
                    baseUrisPromise.resolve();

                } else {
                    util.ajax(
                        {
                            url: this.settings.cloud_host + "api/base_uris",
                            headers: {
                                Accept: this.GET_headers().Accept,
                                "x-api-client-id": this.GET_headers()["x-api-client-id"]
                            }
                        }
                    ).then(
                        this.proxy(function (uris) {
                            this.settings.cloud_api = uris.api;
                            this.settings.files_host = uris.files;
                            this.settings.fillsign_api = uris.fss;
                            this.settings.ims_host = uris.ims;
                            this.settings.cpdf_api = uris.cpdf;
                            baseUrisPromise.resolve();
                        }),
                        function () {
                            baseUrisPromise.reject();
                        }
                    );
                }
                return baseUrisPromise.promise();
            };

            this.baseUris = function () {
                var p = util.Deferred();
                this.cloudBaseUris().done(
                    this.proxy(function () {
                        this.filesBaseUris().done(
                            this.proxy(function () {
                                p.resolve();
                            })
                        );
                    })
                );
                return p.promise();
            };

            this.connect = function () {
                var connectPromise = util.Deferred();
                if (!this.settings.auth_code) {
                    connectPromise.reject();
                    return connectPromise.promise();
                }
                this.baseUris().then(
                    this.proxy(function () {
                        // get an auth token from an auth code...
                        if (this.settings.auth_code) {
                            var params = {
                                grant_type: "authorization_code",
                                code: this.settings.auth_code,
                                client_id: this.settings.ims_client_id,
                                client_secret: this.settings.ims_client_secret
                            };
                            util.ajax(
                                {
                                    url: this.settings.ims_host + "ims/token/v1",
                                    type: "POST",
                                    data: params,
                                    contentType:
                                        'application/x-www-form-urlencoded;' +
                                        'charset=UTF-8'
                                }
                            ).then(
                                this.proxy(function (data) {
                                    var now = (new Date()).getTime(),
                                        isAdobe = /@adobe(test)?\.com$/i.test(data.email);
                                    // clearing auth_code prevents re-query
                                    this.settings.auth_code = null;
                                    if (!isAdobe) {
                                        showMessage(
                                            "PDF Helper Extension is available" +
                                                " to Adobe Employees only"
                                        );
                                        uninstall();
                                    } else {
                                        this.settings.auth_token = "Bearer " +
                                            data.access_token;
                                        this.settings.refresh_token = data.refresh_token;
                                        this.settings.token_expiry =
                                            now + data.expires_in;
                                        this.settings.refresh_time =
                                            now + (data.expires_in / 2);
                                        this.settings.displayName = data.displayName;
                                        connectPromise.resolve();
                                        util.consoleLog("got auth token");
                                    }
                                }),
                                this.proxy(function () {
                                    this.noToken(connectPromise);
                                })
                            );
                        } else {
                            this.noToken(connectPromise);
                        }
                    }),
                    this.proxy(function () {
                        this.noToken(connectPromise);
                    })
                );
                return connectPromise.promise();
            };

            this.refreshToken = function (forceRefresh) {
                var now = (new Date()).getTime(),
                    refreshPromise = util.Deferred(),
                    params;

                if (!this.settings.refresh_token) {
                    // no token?  nothing to refresh...
//                    util.consoleLog("no token");
                    refreshPromise.reject();
                } else if (!forceRefresh && now < this.settings.refresh_time) {
                    // too soon to refresh
//                    util.consoleLog("too soon");
                    refreshPromise.resolve();

                } else if (now > this.settings.token_expiry) {
                    // too late to refresh
//                    util.consoleLog("too late");
                    refreshPromise.reject();

                } else {
//                    util.consoleLog("refresh");
                    // time to refresh
                    params = {
                        grant_type: "refresh_token",
                        refresh_token: this.settings.refresh_token,
                        client_id: this.settings.ims_client_id,
                        client_secret: this.settings.ims_client_secret
                    };

                    util.ajax(
                        {
                            url: this.settings.ims_host + "ims/token/v1",
                            type: "POST",
                            data: params,
                            contentType:
                                'application/x-www-form-urlencoded;' +
                                ' charset=UTF-8'
                        }
                    ).then(
                        this.proxy(function (data) {
                            var now = (new Date()).getTime();
                            this.settings.auth_token = "Bearer " + data.access_token;
                            this.settings.refresh_token = data.refresh_token;
                            this.settings.token_expiry = now + data.expires_in;
                            this.settings.refresh_time = now + (data.expires_in / 2);
                            refreshPromise.resolve();
                            util.consoleLog("refresh token result");
                            util.consoleLogDir(data);
                        }),
                        this.proxy(function () {
                            this.noToken(refreshPromise);
                        })
                    );
                }
                return refreshPromise.promise();
            };

            this.ajaxReady = function (forceRefresh) {
                var getRootPromise = util.Deferred();
                if (this.settings.auth_token) {
                    this.refreshToken(forceRefresh).then(
                        this.proxy(function () {
                            getRootPromise.resolve();
                        }),
                        this.proxy(function () {
                            this.noToken(getRootPromise);
                        })
                    );
                } else {
                    this.connect().then(
                        this.proxy(function () {
                            if (this.settings.files_root) {
                                getRootPromise.resolve();
                                return;
                            }
                            util.ajax({
                                url: this.settings.files_api + "root",
                                type: "GET",
                                headers: this.GET_headers()
                            }).then(
                                this.proxy(function (data) {
                                    // TODO: update the cookie! and settings
                                    // TODO: track the time to refresh
                                    // data looks like:
                                    // expiration_time (seconds)
                                    // primary_token
                                    getRootPromise.resolve();
                                    this.settings.files_root = data.id;
                                }),
                                this.proxy(function () {
                                    this.noToken(getRootPromise);
                                })
                            );
                        }),
                        this.proxy(function () {
                            this.noToken(getRootPromise);
                        })
                    );
                }
                return getRootPromise.promise();
            };

            this.sso_url = function (path) {
                return util.ajax({
                    url: this.settings.cloud_api + "session/sso_uri?path=" + path,
                    type: "GET",
                    headers: this.GET_headers()
                });
            };

            this.authorize = function (code) {
                this.settings.auth_code = code;
                this.settings.auth_token = null;
                delete this.settings.refresh_token;
                delete this.settings.token_expiry;
                delete this.settings.refresh_time;
                return this.ajaxReady();
            };

            this.clearAuth = function () {
                if (!this.settings.auth_token) {
                    return;
                }
                util.ajax(
                    {
                        url: this.settings.cloud_api + "session",
                        type: "DELETE",
                        headers: $POST_headers
                    }
                );

                delete this.settings.auth_token;
                delete this.settings.refresh_token;
                delete this.settings.token_expiry;
                delete this.settings.refresh_time;
                delete this.settings.displayName;
            };

            this.LOG = function (params, severity) {
                // TODO:  throttling?  Limit by elapsed time?
                var length = 0, logPromise = util.Deferred();

                if (SETTINGS.USE_ACROBAT) {
                    return logPromise.resolve();
                }
                // 10: debug, 20: info, 30: warning, 40: error
                severity  = severity || 20;
                console.log("LOG: " + JSON.stringify(params));
                function failed(err) {
                    console.log("Failed to log to splunk");
                    console.dir(err);
                    logPromise.reject();
                }

                this.ajaxReady().then(
                    function () {
                        this.util.ajax(
                            {
                                url:   this.settings.files_api +
                                       "send_log_event",
                                type:  "POST",
                                data:  JSON.stringify(
                                    {
                                        level: severity,
                                        message: JSON.stringify(params)
                                    }
                                ),
                                headers:        this.POST_headers(),
                                processData:    false
                            }
                        ).then(
                            function () {
                                logPromise.resolve(params);
                            },
                            failed
                        );
                    }.bind(this),
                    failed
                );
                return logPromise.promise();
            }.bind(this);

        };

    if (!singleton) {
        singleton = new Common();
        util.ajaxError(function (e, xhr, args, errorText) {
            if (xhr.status === 401) {
                singleton.clearAuth();
            }
        });
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
