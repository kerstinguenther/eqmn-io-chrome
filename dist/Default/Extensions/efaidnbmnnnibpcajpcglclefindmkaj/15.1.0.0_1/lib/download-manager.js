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
/*jslint devel: true, browser: true, nomen: true, maxlen: 93 */

function dependOn() {
    'use strict';
    return [
        require("communicate"),
        require("common"),
        require("util"),
        require("proxy"),
        require("analytics"),
        require("feat")
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
def(dependOn(), function (communicate, common, util, proxy, analytics, feat) {
    'use strict';
    var singleton = null,
        DownloadManager,
        prop,
        upload,
        session;

    upload = function () {
        return communicate.getModule("upload");
    };

    session = function () {
        return communicate.getModule("session");
    };


    DownloadManager = function () {
        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        this.uploadHandler = function (result) {
            var createContentTypes = [
                'application/illustrator',
                'image/bmp',
                'application/msword',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'application/vnd.adobe.form.fillsign',
                'image/gif',
                'application/x-indesign',
                'image/jpeg',
                'image/jpeg',
                'application/vnd.oasis.opendocument.formula',
                'application/vnd.oasis.opendocument.graphics',
                'application/vnd.oasis.opendocument.presentation',
                'application/vnd.oasis.opendocument.spreadsheet',
                'application/vnd.oasis.opendocument.text',
                'image/png',
                'application/vnd.ms-powerpoint',
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                'application/mspowerpoint',
                'application/postscript',
                'image/vnd.adobe.photoshop',
                'application/vnd.ms-publisher',
                'application/x-tika-msoffice',
                'text/rtf',
                'application/vnd.sun.xml.writer.template',
                'application/vnd.sun.xml.draw',
                'application/vnd.sun.xml.calc',
                'application/vnd.sun.xml.impress',
                'application/vnd.sun.xml.writer',
                'text/plain',
                'image/tiff',
                'image/tiff',
                'text/plain',
                'application/vnd.ms-excel',
                'application/msexcel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/zip'
            ],
                request = result.request;

            if (result.id) {
                request.assetId = result.id;
                util.ajax({
                    url: common.settings.files_api + "assets/" + result.id + "/metadata",
                    type: "GET",
                    headers: common.GET_headers()
                }).done(
                    this.proxy(function (data) {

                        delete request.create_path;
                        delete request.export_path;
                        delete request.form_path;
                        delete request.preview_path;

                        request.preview_path = "file/" + result.id;
                        request.send_path = "send/file/" + result.id;
                        request.fillsign = false;
                        analytics.event(analytics.e.UPLOAD_COMPLETE);

                        if (data.content_type === "application/pdf") {
                            request.export_path = "exportpdf/" + result.id;
                            request.fillsign = true;
                        }
                        //if (data.content_type === "application/zip") {
                            //do nothing for now
                        //}
                        if (createContentTypes.indexOf(data.content_type) !== -1) {
                            request.create_path = "convertpdf/" + result.id;
                            request.fillsign = false;
                        }
                        if (request.handleResult === "fillsign") {
                            delete request.handleResult;
                            this.fill_sign(request);

                        } else if (request.handleResult) {
                            util.consoleLog("handleResult: " + request.handleResult);

                            var path = {
                                "preview"       : request.preview_path,
                                "image_preview" : request.preview_path,
                                "export"        : request.export_path,
                                "send"          : request.send_path,
                                "to_pdf"        : request.create_path
                            }[request.handleResult];

                            analytics.event(analytics.e.REDIRECT);
                            delete request.handleResult;
                            session().gotoPath(path);

                        }
                    })
                );
            }
        };

        this.do_upload = function (request, sender) {
            var analyticsStr = {
                "upload-image"  : analytics.e.CONTEXT_UPLOAD_IMAGE,
                "upload_link"   : analytics.e.CONTEXT_UPLOAD_LINK,
                "upload"        : analytics.e.CONTEXT_UPLOAD_PDF_PAGE
            }[request.menuItem];

            if (analyticsStr) {
                analytics.event(analyticsStr);
                delete request.menuItem;
            }
            upload().upload(request).done(this.proxy(this.uploadHandler));
        };

        this.sign_out = function (request, sender) {
            session().signOut();
        };

        this.dismiss = function (request, sender) {
            util.consoleLog("dismiss/ok");
        };

        this.specialCases = SETTINGS.USE_FLICKR ? [
            {
                regex: /http[s]:\/\/www\.flickr\.com/,
                action: "flickr",
                lastPromptTime: null
            }
        ] : [];

        this.handleSpecialUrl = function (url, tabId) {
            var handled = false;
            util.each(this.specialCases, function (index, special) {
                if (special.regex.test(url)) {
                    var invite = true, now = (new Date()).getTime();
                    // For now, wait 10 seconds before prompting again.
                    // TODO: in the future, this interval should probably be about a week...
                    if (special.lastPromptTime &&
                            ((now - special.lastPromptTime) < 1000 * 10)) {
                        invite = false;
                    }
                    if (invite) {
                        special.lastPromptTime = now;
                        util.consoleLog("INVITE: " + now);
                        communicate.deferMessage({panel_op: special.action, tabId: tabId});
                    }

                    handled = true;
                }
            });
            return handled;
        };

        this.fill_sign = function (request, sender) {
            var promise = request.userSelectPromise || util.Deferred().resolve().promise();
            promise.then(this.proxy(function () {
                util.consoleLog("fill and sign");

                var options = {
                    url            : common.settings.fillsign_api + "createform",
                    contentType    : "application/json",
                    accept      : common.GET_headers().Accept,
                    type        : "POST",
                    dataType    : "json",
                    xhrFields    : { withCredentials: true },
                    headers        : common.POST_headers()
                };

                options.data = JSON.stringify({ "asset_id": request.assetId});
                session().message("Preparing for Fill and Sign", true);

                analytics.event(analytics.e.CREATE_FORM_PROGRESS_SHOWN);
                util.ajax(options).then(
                    this.proxy(function (data) {
                        util.consoleLog("form created");
                        util.consoleLogDir(data);

                        request.form_path = "fillsign/" + data.form_id;
                        analytics.event(analytics.e.CREATE_FORM_COMPLETE);
                        analytics.event(analytics.e.REDIRECT);
                        session().gotoPath(request.form_path);
                    }),

                    this.proxy(function (xhr) {
                        util.consoleLog("form create failed");
                        proxy.REST_error(xhr, this);
                        return xhr;
                    })
                );
            }));
        };

        this.newTab = function (url, tabId, contentType) {
            var request = {
                panel_op    : "html_menu",
                hidden      : true,
                tabId       : tabId
            },
                anl,
                optionsURL = chrome.runtime.getURL("data/js/options.html");

            if (url.indexOf(common.settings.redirect_uri + "?code=") === 0) {
                session().foundCode(url);
                return;
            }

            // if the options page has been opened, make sure it's initialized
            // with the current active environment
            if (url.includes(optionsURL)) {
                anl = analytics.getAnalyticsUsage();

                if (url.includes("?os=mac")) {
                    optionsURL += "?os=mac";
                } else {
                    optionsURL += "?anl=" + anl;
                }
                if (!SETTINGS.USE_ACROBAT) {
                    optionsURL += "&env=" + common.settings.env;
                }
                if (url !== optionsURL) {
                    chrome.tabs.update(tabId, {url: optionsURL, active: true});
                    return;
                }
                analytics.event(analytics.e.OPTIONS_PAGE);
                return;
            }

            session().checkSessionTab(tabId, url);
            if (communicate.avoidUrl(url)) {
                communicate.disable(tabId);
                return;
            }
            if (singleton.handleSpecialUrl(url, tabId)) {
                return;
            }
            communicate.loaded(tabId);
        };
        this.startup = function (request, sender) {
            // check if this is the first active tab in a new chrome
            // session.  If so, register it as a new tab.
            if (!this.started) {
                this.newTab(
                    request.url,
                    sender.tab.id,
                    request.is_pdf ? "application/pdf" : "text/html"
                );
                this.started = true;
            }
            if (request.is_pdf) {
                communicate.pdf_menu(request, sender);
            }
        };
    };

    if (!singleton) {
        singleton = new DownloadManager();
        communicate.registerModule("download-manager", singleton);
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
    communicate.registerHandlers(
        {
            "do_upload"     : singleton.proxy(singleton.do_upload),
            "dismiss"       : singleton.proxy(singleton.dismiss),
            "ok"            : singleton.proxy(singleton.dismiss),
            "fillsign"      : singleton.proxy(singleton.fill_sign),
            "sign-out"      : singleton.proxy(singleton.sign_out),
            "html-startup"  : singleton.proxy(singleton.startup),
            "pdf-menu"      : singleton.proxy(singleton.startup)
        }
    );
    if (util.isChrome()) {
        // check for the IMS token parameter
        // check for special URLs e.g. flickr
        chrome.tabs.onUpdated.addListener(singleton.proxy(function (tabId, info, tab) {
            if (info.status === "complete") {
                //util.consoleLog("tab loaded: " + tab.url);
                singleton.newTab(tab.url, tabId);
			} else if (info.status === "loading") {
                communicate.loading({id: tabId});
			}
	    }));
    }
    if (util.isFF()) {
        singleton.proxy(function () {
            var Cu = require("chrome").Cu,
                downloads = Cu["import"]("resource://gre/modules/Downloads.jsm").Downloads,
                task = Cu["import"]("resource://gre/modules/Task.jsm").Task,

                view = {
                    onDownloadAdded: function (download) {
                        util.consoleLog("Added", download);
                        util.consoleLog("Added Content type: " + download.contentType);

                        if (communicate.avoidUrl(download.source.url)) {
                            return;
                        }

                        var request = {
                            filename    : download.target.path.replace(/\S*(\\|\/)/, ""),
                            url         : download.source.url,
//                          tabId       : details.tabId,
                            panel_op    : "pdf_menu"
                        };

                        communicate.deferMessage(request);
                    }
                };

            task.spawn(function () {
                try {
                    downloads.getList(downloads.ALL).then(
                        function (list) {
                            util.consoleLogDir(list);
                            list.addView(view);
                        }
                    );
                } catch (ex) {
                    util.consoleError(ex);
                }
            });
        })();

        require("sdk/tabs").on('ready', function (tab) {
            var code, request, url = tab.url;
            singleton.newTab(tab.url, tab.id, tab.contentType);
        });
        // this import will put some useful services in global scope. e.g. Blob
        require("chrome").Cu["import"]('resource://gre/modules/Services.jsm');
    }
    return singleton;
});
