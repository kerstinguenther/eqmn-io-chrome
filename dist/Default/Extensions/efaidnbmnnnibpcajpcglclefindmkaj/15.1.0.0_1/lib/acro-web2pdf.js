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
/*jslint devel: true, browser: true, nomen: true, maxlen: 80, bitwise: true */

/**
* This file is a place holder for all conversion functions and their interaction
* with the chrome native interface
*/

function dependOn() {
    'use strict';
    return [
        require("communicate"),
        require("proxy"),
        require("common"),
        require("util"),
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

def(dependOn(), function (communicate, proxy, common, util, analytics) {
    'use strict';
    var singleton = null,
        prop,
        NativeMessagingClass,
        Web2pdf_Conversion,
        gstate,
        track = false,
        TASK_ID_CONVERTTOPDF = 0,
        TASK_ID_APPENDTOPDF = 1,
        TASK_ID_SHOWCONVERSIONSETTINGSDIALOG = 2,
        TASK_ID_FEATCONVERTTOPDF = 3,
        TASK_ID_FEATAPPENDTOPDF = 4,
        TASK_ID_WRITEFILETODISK = 10,
        TASK_ID_OPEN_PDF = 11,
        TASK_ID_OPEN_DOWNLOADED_PDF = 12,
        TASK_ID_GET_SHIM_VERSION = 13,
        TASK_ID_EXIT_SHIM = 14,
        legacyTasks = [
            TASK_ID_CONVERTTOPDF,
            TASK_ID_APPENDTOPDF,
            TASK_ID_SHOWCONVERSIONSETTINGSDIALOG,
            // not supported, but we allow it to be attempted:
            TASK_ID_GET_SHIM_VERSION,
            // Exit might be supported.
            // It's been back-ported to A11 and classic.
            TASK_ID_EXIT_SHIM
        ],
        exitTasks = [
            TASK_ID_SHOWCONVERSIONSETTINGSDIALOG = 2,
            TASK_ID_OPEN_PDF = 11,
            TASK_ID_OPEN_DOWNLOADED_PDF = 12,
            TASK_ID_GET_SHIM_VERSION = 13
        ];

    gstate = function () { return communicate.getModule("acro-gstate"); };

    Web2pdf_Conversion = function () {
        var conversions = [],
            m_NativeConnectionPort = null;

        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        //enums for conversion settings
        //if any change is made in these, then the enum in the
        // C++ code must also be changed.
        this.UNSET = 0;
        this.OPEN_IN_ACROBAT = 1;
        this.APPEND = 2;                    // 1 << 1;
        this.CONVERT_PAGE = 4;              // 1 << 2;
        this.CONVERT_LINK = 8;              // 1 << 3;
        this.CONVERT_SELECTION = 16;        // 1 << 4;
        this.PRINT = 32;                    // 1 << 5;
        this.EMAIL = 64;                    // 1 << 6;
        this.CALLER_TOOLBAR = 128;          // 1 << 7;
        this.CLEAN_FILE_ON_FAILURE = 256;   // 1 << 8;

        //enums for reporting conversion status
        //if any change is made in these, then the enum
        // in the C++ code must also be changed.
        this.STATUS_WAITING = 10000;
        this.STATUS_DOWNLOADING = 10001;
        this.STATUS_CONVERTING = 10002;
        this.STATUS_SUCCESS = 10003;
        this.STATUS_ERROR = 10004;
        this.STATUS_NOINSTANCE = 10005;
        this.STATUS_FILELOCKED = 10006;
        this.STATUS_NOACROBAT = 10007;
        this.STATUS_CANCELLED = 10008;
        this.STATUS_FILE_OPENED = 10100;
        this.STATUS_FILE_OPEN_FAILED = 10101;

        this.imagePromise = null;


        this.errorHandler = function () {
            try {
                if (conversions.length === 0) {
                    return;
                }

                this.Done(
                    conversions[0].tabId,
                    this.STATUS_ERROR,
                    null,
                    util.getTranslation("web2pdfHTMLJSError")
                );
            } catch (e) {
            }
        };

        // if there's an uncaught javascript error, abort the current
        // conversion.
        proxy.handlers(this.errorHandler.bind(this));

        this.Done = function (tabId, state, filePath, msg) {
            if (tabId === -1) {
                // tabId can be -1 if acrobat exits unexpectedly.
                // in that case we'll abort the current conversion
                // (if there is one)
                if (conversions.length === 0) {
                    return;
                }
                tabId = conversions[0].tabId;
            }
            this.setStatus(tabId, state, filePath, msg);
            this.nextConversion(tabId);
        };

        this.nextConversion = function (tabId) {
            var i;
            for (i = 0; i < conversions.length; i += 1) {
                if (conversions[i].tabId === tabId) {
                    conversions.splice(i, 1);
                }
            }
            // start the next conversion if there is one waiting
            if (conversions.length > 0) {
                conversions[0].start.resolve(conversions[0]);
            }
            this.exitShim();
        };

        function getRequest(id) {
            var i;
            for (i = 0; i < conversions.length; i += 1) {
                if (conversions[i].tabId === id) {
                    return conversions[i];
                }
            }
            return null;
        }

        this.cancelConversion = function (tabId) {
            var i;
            for (i = 0; i < conversions.length; i += 1) {
                if (conversions[i].tabId === tabId) {
                    if (conversions[i].start) {
                        conversions[i].start.reject();
                    }
                    conversions.splice(i, 1);
                }
            }
        };

        this.addConversion = function (request, waiting) {
            request.start = util.Deferred();
            conversions.push(request);

            if (conversions.length === 1) {
                request.start.resolve(request);
            } else {
                if (waiting) {
                    waiting();
                }
            }
            return request.start;
        };

        this.setStatus = function (id, status, filePath, msg) {
            var state,
                request = getRequest(id) || this.openPDFRequest,
                start = new Date();

            util.consoleLog("setStatus: " + id + " status: " + status);

            if (!request) {
                util.consoleLog("failed to find conversion for tabId: " + id);
                return;
            }
            if (!request.timing) {
                request.timing = [];
            }

            if (status === this.STATUS_WAITING) {
                state = "waiting";
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_WAITING);
                //start tracking time analytics for WAIT
                request.timing.push({stage: "WAIT",
                                     start_time: start.getTime()});

            } else if (status === this.STATUS_DOWNLOADING) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_DOWNLOADING);
                state = "downloading";
                this.logTiming(request.timing, "WAIT");

            } else if (status === this.STATUS_CONVERTING) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_IN_PROGRESS);
                state = "in_progress";
                //finish time tracking for USER_PROMPT
                this.logTiming(request.timing, "USER_PROMPT");
                //start tracking time analytics for CONVERT
                request.timing.push({stage: "CONVERT",
                                      start_time: start.getTime()});

            } else if (status === this.STATUS_SUCCESS) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_COMPLETE);
                state = "complete";
                this.logTiming(request.timing, "CONVERT");

            } else if (status === this.STATUS_ERROR) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_FAILED);
                state = "failure";

            } else if (status === this.STATUS_CANCELLED) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_CANCELLED);
                state = "cancelled";
                this.logTiming(request.timing, "USER_PROMPT");

            } else if (status === this.STATUS_NOACROBAT) {
                analytics.event(analytics.e.TREFOIL_HTML_CONVERT_NO_ACROBAT);
                state = "noacrobat";

            } else if (status === this.STATUS_FILELOCKED) {
                state = "filelocked";

            } else if (status === this.STATUS_FILE_OPENED) {
                analytics.event(analytics.e.TREFOIL_PDF_DOWNLOAD_OPENED);
                state = "pdf_opened";

            } else if (status === this.STATUS_FILE_OPEN_FAILED) {
                analytics.event(analytics.e.TREFOIL_PDF_DOWNLOAD_OPEN_FAILED);
                state = "pdf_open_failed";

            } else {
                util.consoleLog("Unexpected status: " + status);
                state = "unknown";
            }

            request.panel_op = "status";
            request.current_status = state;
            if (filePath) {
                request.file_path = filePath;
            }
            if (msg) {
                request.message = msg;
            }
            communicate.deferMessage(request);
        };

        function getMessageCallback(messageID) {
            // The shim can ask for some specific message strings.
            // The strings we need to support include:
            // "web2pdfReplaceWarning"
            // "web2pdfExtnName"
            // "web2pdfNoWritePermissionOnSelectedFile"

            return chrome.i18n.getMessage(messageID) ||
                        chrome.i18n.getMessage("web2pdfStatusError");
        }

        this.nativeMessageCallback = function (msg) {
            var path;
//            util.consoleLogDir(msg);

            if (msg.messageType === "setStateCallback") {
                this.setStatus(+msg.conversionID, +msg.state);

            } else if (msg.messageType === "doneCallback") {
                if (msg.filePath) {
                    path = util.atob16(msg.filePath);
                }
                this.Done(+msg.conversionID, +msg.state, path);

            } else if (msg.messageType === "getMessageCallback") {
                this.sendMessageToNative(
                    {message: getMessageCallback(msg.msgIDStr)}
                );

            } else if (msg.messageType === "saveSuccessCallback") {
                path = util.atob16(msg.path);
                this.imagePromise.resolve(path);

            } else if (msg.messageType === "saveFailureCallback") {
                util.consoleLogDir("failed to save image");
                this.imagePromise.reject();

            } else if (msg.messageType === "shimVersionInfo") {
                if (this.versionCallback) {
                    this.versionCallback(msg);
                    delete this.versionCallback;
                }
            } else if (msg.messageType === "fileOpenCallback") {
                if (+msg.state === 0) {
                    this.setStatus(0, this.STATUS_FILE_OPENED);
                } else {
                    this.setStatus(0, this.STATUS_FILE_OPEN_FAILED);
                }
                if (this.openPDFRequest) {
                    // it appears this sometimes gets called when we
                    // launch a converted file.
                    this.nextConversion(this.openPDFRequest.tabId);
                    delete this.openPDFRequest;
                }
            }
        };

        this.init = function () {
            this.m_NativeConnectionPort = chrome.runtime.connectNative(
                'com.adobe.acrobat.chrome_webcapture'
            );
            this.m_NativeConnectionPort.onMessage.addListener(
                this.proxy(this.nativeMessageCallback)
            );
            this.m_NativeConnectionPort.onDisconnect.addListener(
                this.proxy(function () {
                    var status = this.STATUS_ERROR, entry;
                    if (chrome.runtime.lastError.message ===
                            "Specified native messaging host not found.") {
                        if (this.versionCallback) {
                            this.versionCallback({
                                "messageType" : "shimVersionInfo",
                                "majorVersion" : "0",
                                "minorVersion" : "0"
                            });
                            delete this.versionCallback;
                        }
                        status = this.STATUS_NOACROBAT;
                    }
                    if (chrome.runtime.lastError.message ===
                            "Native host has exited") {
                        status = this.STATUS_ERROR;
                    }
                    this.m_NativeConnectionPort = null;
                    this.Done(-1, status);
                })
            );
        };

        this.sendMessageToNative = function (message) {
            // don't attempt any operations not supported by the
            // legacy shim.  As far as we know, we've disabled these
            // upstream -- but this check is here for insurance.
            if (communicate.legacyShim() &&
                    message.task &&
                    legacyTasks.indexOf(message.task) === -1) {
                util.consoleLog("Skipping task: " +
                                message.task +
                                " in legacy shim"
                               );
                return;
            }
            if (!this.m_NativeConnectionPort) {
                this.init();
            }
            this.m_NativeConnectionPort.postMessage(message);
            // We don't like having the shim hang around.  One of the
            // side-effect is that conversions will fail to bring acrobat
            // to the foreground.  Another side effect is that we get
            // a timeout message when the port times out.
            // So for any request that's not related to a conversion,
            // ask the shim to exit.
            if (exitTasks.indexOf(message.task) !== -1) {
                setTimeout(this.proxy(function () {
                    this.exitShim();
                }), 1000);
            }
        };

        this.exitShim = function () {
            if (conversions.length === 0) {
                this.sendMessageToNative({task: TASK_ID_EXIT_SHIM});
            }
        };

        this.getVersion = function (callback) {
            this.versionCallback = callback;
            this.sendMessageToNative({task: TASK_ID_GET_SHIM_VERSION});
        };

        this.openInAcrobat = function (request) {
            this.addConversion(request).then(
                this.proxy(function (request) {
                    this.openPDFRequest = request;
                    this.sendMessageToNative(
                        {
                            task: TASK_ID_OPEN_DOWNLOADED_PDF,
                            pdfData: request.base64PDF.split(",")[1],
                            fileName: request.filename
                        }
                    );
                    //log analytics for PDF size in MB
                    analytics.checkAndLogPDFSize(
                        request.base64PDF.length / 1048576
                    );
                    delete request.base64PDF;
                })
            );
        };

        this.openFile = function (request) {
            // don't harass acrobat with this request if there
            // are active conversions.
            if (conversions.length === 0) {
                this.sendMessageToNative(
                    {
                        task: TASK_ID_OPEN_PDF,
                        filePath: request.file_path
                    }
                );
            }
        };

        this.SendForConversion = function (conversion, request) {
            try {

                var task, result, start = new Date(), timings = [];
                if ((conversion.conversionSettings & this.APPEND) !== 0) {
                    task = TASK_ID_APPENDTOPDF;
                } else {
                    task = TASK_ID_CONVERTTOPDF;
                }

                if (!request.timing) {
                    request.timing  = [];
                }

                request.timing.push({stage: "USER_PROMPT",
                                 start_time: start.getTime()});

                if (request.outputPath) {
                    this.sendMessageToNative(
                        {
                            task: TASK_ID_FEATCONVERTTOPDF,
                            conversionID: request.tabId,
                            domData: conversion.domData,
                            conversionSettings: conversion.conversionSettings,
                            charset: conversion.charset,
                            url: conversion.url,
                            docTitle: conversion.domtitle,
                            outputPath : request.outputPath
                        }
                    );
                } else {
                    this.sendMessageToNative(
                        {
                            task: task,
                            conversionID: request.tabId,
                            domData: conversion.domData,
                            conversionSettings: conversion.conversionSettings,
                            charset: conversion.charset,
                            url: conversion.url,
                            docTitle: conversion.domtitle
                        }
                    );
                }

            } catch (err) {
                this.Done(request.tabId, this.STATUS_ERROR);
            }
        };

        this.showConversionSettingsDialog = function () {
            // don't try the settings dialog if there are conversions active.
            // TODO: Come up with a better experience than just
            // ignoring the request
            if (conversions.length === 0) {
                this.sendMessageToNative(
                    {task: TASK_ID_SHOWCONVERSIONSETTINGSDIALOG}
                );
            }
        };

        this.convertToPDF = function (conversion, request) {
            var filePath = null, openDocAfterConversion;
            conversion.conversionSettings = this.UNSET;

            // Set conversion Settings
            openDocAfterConversion = gstate().getViewResultsPreferenceState();
            if (openDocAfterConversion) {
                conversion.conversionSettings |=
                    this.OPEN_IN_ACROBAT;
            }

            if (conversion.action === gstate().web2pdfAction.APPEND) {
                conversion.conversionSettings |= this.APPEND;
            } else {
                conversion.conversionSettings |=
                    this.CLEAN_FILE_ON_FAILURE;
            }

            if (conversion.context === gstate().web2pdfContext.PAGE) {
                conversion.conversionSettings |=
                    this.CONVERT_PAGE;
            }

            if (conversion.caller === gstate().web2pdfCaller.TOOLBAR) {
                conversion.conversionSettings |=
                    this.CALLER_TOOLBAR;
            } else {
                if (conversion.context === gstate().web2pdfContext.LINK) {
                    conversion.conversionSettings |=
                        this.CONVERT_LINK;
                }
            }

            this.SendForConversion(conversion, request);
        };

        this.processImages = function (request, i, domData) {
            var ref = request.blob.refs[i], start = new Date();
            if (!request.timing) {
                request.timing = [];
            }

            if (!request.imagesComplete) {
                request.imagesComplete = util.Deferred();
            }
            this.imagePromise = util.Deferred();

            if (i >= request.blob.refs.length) {
                this.imagePromise.resolve();
                request.imagesComplete.resolve();
                return this.imagePromise;
            }

            // console.log(i.toString() + " " + ref.type + " " + !!ref.data);
            if (!ref.data || ref.data === "data:") {
                request.blob.refs.splice(i, 1);
                this.imagePromise.resolve();

            } else if (ref.type === "image") {
                if (!track) {
                    request.timing.push({stage: "SEND_IMAGES",
                                         start_time: start.getTime()});
                    track = true;
                }

                request.blob.refs.splice(i, 1);

                this.sendMessageToNative({
                    task:           TASK_ID_WRITEFILETODISK,
                    // grab the base64 portion of the data uri
                    imagedata:      ref.data.split(",")[1],
                    conversionID:   request.tabId
                });

                this.imagePromise.then(
                    this.proxy(function (path) {
                        domData.push("<AcroexchangeDownloadSeprator " +
                            "AcroexchangeDownloadUrl=" +
                            ref.placeholder +
                            "><FILEPATH>" +
                            path +
                            "</FILEPATH></AcroexchangeDownloadSeprator>");
                    }),
                    function () {
                    }
                );
            } else {
                this.imagePromise.resolve();
                i += 1;
            }
            this.imagePromise.done(
                this.proxy(function () {
                    this.processImages(request, i, domData);
                })
            );
            return this.imagePromise;
        };

        // Process the cloned DOM we've received from get-html
        this.acro_html = function (r, sender) {
            var i,
                domData,
                request;

            if (r.error) {
                util.consoleError(r.error);
                analytics.logError(r.error_analytics);

                this.Done(
                    r.tabId,
                    this.STATUS_ERROR,
                    null,
                    util.getTranslation(r.error)
                );
            } else {
                //analytics for blob size (size in MB)
                analytics.checkAndLogHTMLBlobSize(r.blob.currentSize / 1048576);
                analytics.logContents(r);
                //log analytics for html content
                if (r.analytics) {
                    analytics.setParamsAndLogAnalytics(
                        r.analytics,
                        analytics.e.HTML_SOURCE_CONTENT,
                        "content"
                    );
                }
                //log analytics for dom clone timing
                analytics.setArg("stage", "CLONE");
                analytics.checkAndLogTimingRange(r.cloneTiming);

                request = getRequest(r.tabId);
                request.blob = r.blob;
                domData = [];
                track = false;

                this.processImages(request, 0, domData);
                request.imagesComplete.then(
                    this.proxy(function () {
                        delete request.imagesComplete;
                        this.logTiming(request.timing, "SEND_IMAGES");
                        domData.push(
                            "<AcroexchangeDownloadSeprator " +
                                "AcroexchangeDownloadUrl=" +
                                sender.tab.url +
                                ">" +
                                request.blob.html +
                                "</AcroexchangeDownloadSeprator>"
                        );
                        for (i = 0; i < request.blob.refs.length; i += 1) {
                            domData.push(
                                "<AcroexchangeDownloadSeprator " +
                                    "AcroexchangeDownloadUrl=" +
                                    request.blob.refs[i].placeholder +
                                    ">" +
                                    request.blob.refs[i].data +
                                     "</AcroexchangeDownloadSeprator>"
                            );
                        }
                        // free a big chunk of memory...
                        delete request.blob;
                        this.convertToPDF(
                            {
                                caller      : request.caller,
                                action      : request.action,
                                context     : gstate().web2pdfContext.PAGE,
                                domData     : domData.join("\n"),
                                charset     : "UTF-8",
                                domtitle    : request.domtitle,
                                url         : request.url
                            },
                            request
                        );
                    })
                );
            }
        };

        this.logTiming = function (timing, stage) {
            var ms, end = new Date();
            timing.forEach(function (t) {
                if (t.stage === stage) {
                    ms = end.getTime() - t.start_time;
                    analytics.setArg("stage", stage);
                    //log the times in 1/10 second units
                    analytics.checkAndLogTimingRange(ms / 100);
                }
            });
        };


        this.handleConversionRequest = function (request) {
            var initScript,
                waiting = this.proxy(function () {
                    this.setStatus(request.tabId, this.STATUS_WAITING);
                });

            if (!communicate.legacyShim() &&
                    request.context === gstate().web2pdfContext.PAGE) {
                this.addConversion(
                    request,
                    waiting
                ).then(this.proxy(function (request) {

                    delete request.start;
                    this.setStatus(request.tabId, this.STATUS_DOWNLOADING);

                    //log analytics for protocol and
                    analytics.logSiteAndProtocolAnalytics(request.url);

                    initScript = "var maxSize = " + SETTINGS.MAX_HTML_SIZE +
                        ", DEBUG = " + SETTINGS.DEBUG_MODE +
                        ", TABID = " + request.tabId +
                        ", OP = 'acro-html'" +
                        ", EXCLUDE = ['font', 'svg_image'];";

                    chrome.tabs.executeScript(
                        request.tabId,
                        {
                            code: initScript,
                            runAt: "document_start"
// TODO: until our iframe support is fixed, run only on the main document
//                            allFrames: true
                        },
                        this.proxy(function () {
                            if (chrome.runtime.lastError) {
                                throw new Error(
                                    chrome.runtime.lastError.message
                                );
                            }
                            chrome.tabs.executeScript(
                                request.tabId,
                                {
                                    file: "data/js/get-html.js"
// TODO: until our iframe support is fixed, run only on the main document
//                                    allFrames: true
                                }
                            );
                        })
                    );
                }));
            } else if (communicate.legacyShim() ||
                        request.context === gstate().web2pdfContext.LINK) {
                this.addConversion(
                    request,
                    waiting
                ).then(this.proxy(function (request) {
                    this.convertToPDF(
                        {
                            caller   : request.caller,
                            action   : request.action,
                            context  : request.context,
                            domData  : "",
                            charset  : "UTF-8",
                            domtitle : request.domtitle,
                            url      : request.url
                        },
                        request
                    );
                }));
            }
        };

        function getTitle(title) {
            title = title ||
                chrome.i18n.getMessage("web2pdfUntitledFileName");

            return title.replace(/[<>?:|\*"\/\\'&\.]/g, "");
        }

        this.convertToPDFPopupMenu = function (request, sender) {
            var command =
                {
                    tabId     : sender.tab.id,
                    caller    : gstate().web2pdfCaller.TOOLBAR,
                    action    : gstate().web2pdfAction.CONVERT,
                    context   : gstate().web2pdfContext.PAGE,
                    url       : sender.tab.url,
                    domtitle  : getTitle(sender.tab.title)
                };

            // If it's a FEAT test, outputPath will be provided
            if (request.outputPath) {
                command.outputPath = request.outputPath;
            }
            this.handleConversionRequest(command);
        };

        this.appendToExistingPDFPopupMenu = function (request, sender) {
            this.handleConversionRequest(
                {
                    tabId   : sender.tab.id,
                    caller  : gstate().web2pdfCaller.TOOLBAR,
                    action  : gstate().web2pdfAction.APPEND,
                    context : gstate().web2pdfContext.PAGE,
                    url     : sender.tab.url,
                    domtitle: getTitle(sender.tab.title)
                }
            );
        };
    };

    if (!singleton) {
        singleton = new Web2pdf_Conversion();

        communicate.registerModule("acro-web2pdf", singleton);
        communicate.registerHandlers(
            {
                "acro-html" : singleton.proxy(singleton.acro_html),

                "appendToExistingPDFPopupMenu":
                    singleton.proxy(singleton.appendToExistingPDFPopupMenu),

                "convertToPDFPopupMenu":
                    singleton.proxy(singleton.convertToPDFPopupMenu),

                "showConversionSettingsDialog":
                    singleton.proxy(singleton.showConversionSettingsDialog)
            }
        );
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
