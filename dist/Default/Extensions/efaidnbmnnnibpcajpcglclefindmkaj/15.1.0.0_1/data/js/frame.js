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

/*global window, $, console, util, addon, acom_analytics, chrome, SETTINGS */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

var request,
    timer,
    timer_off = false,
    initialized = false,
    events = acom_analytics.e,
    MENU_TIME = 1500,
    PDF_MENU_TIME = 3000,
    FADE_TIME = 1500,
    sendCancel,
    isChecked;

function sendMessage(request, keep) {
    'use strict';

    // remove the trefoilClick property or else
    // the content script picks up the request and
    // deletes the iframe
    delete request.trefoilClick;
    util.messageToMain(request);
    delete request.analytics;
    if (timer) {
        clearTimeout(timer);
        timer = null;
    }
    // make sure the menu gets dismissed
    if (!keep && request.content_op !== "dismiss") {
        sendCancel();
    }
}

function analytics(str) {
    'use strict';
    if (!request.analytics) {
        request.analytics = [];
    }
    request.analytics.push(str);
}

function clearTimer() {
    'use strict';
//    util.consoleLog("Clear timer: " + timer);
    if (timer) {
        clearTimeout(timer);
        timer = null;
    }
    $(".acrobatMainDiv").stop().css("opacity", 1.0);
}

function sendCancel() {
    'use strict';
//    util.consoleLog("sendCancel");
    request.content_op = "dismiss";
    request.main_op = "relay_to_content";
    sendMessage(request);
}

function fadeAway() {
    'use strict';
//    util.consoleLog("fadeaway");
    timer = null;
    $(".acrobatMainDiv").animate({opacity: 0}, FADE_TIME, "swing", sendCancel);
}

function setTimer() {
    'use strict';
    if (SETTINGS.TEST_MODE || SETTINGS.DEBUG_MODE) {
        // disables the timeout
        timer = 1;
    }
    if (!timer && !timer_off) {
        timer = setTimeout(function () {
            setTimeout(fadeAway);
        }, request.is_pdf ? PDF_MENU_TIME : MENU_TIME);
    }
}

function upload() {
    'use strict';
    request.main_op = "do_upload";
    request.handleResult = "preview";
    analytics(events.PDF_MENU_UPLOAD_CLICKED);
    sendMessage(request);
}

function do_export() {
    'use strict';
    request.main_op = "do_upload";
    request.handleResult = "export";
    analytics(events.PDF_MENU_EXPORT_CLICKED);
    sendMessage(request);
}

function send() {
    'use strict';
    request.main_op = "do_upload";
    request.handleResult = "send";
    analytics(events.PDF_MENU_SEND_CLICKED);
    sendMessage(request);
}

function fillsign() {
    'use strict';
    request.main_op = "do_upload";
    request.handleResult = "fillsign";
    analytics(events.PDF_MENU_FILLSIGN_CLICKED);
    sendMessage(request);
}

function doAcrobat() {
    'use strict';
    request.main_op = "open_in_acrobat";
    analytics(events.TREFOIL_PDF_ACROBAT);
    sendMessage(request, true);
}

function to_html(outputPath) {
    'use strict';
    analytics(events.TREFOIL_HTML_CONVERT_NEW);

    if (!util.getCookie("ViewResultsPref")) {
        analytics(events.TREFOIL_HTML_CONVERT_OPEN_DEFAULT);
    } else if (isChecked) {
        analytics(events.TREFOIL_HTML_CONVERT_OPEN_CHANGED);
    } else {
        analytics(events.TREFOIL_HTML_CONVERT_NO_OPEN);
    }

    if (SETTINGS.USE_ACROBAT) {
        request.main_op = "convertToPDFPopupMenu";
        if (outputPath) {
            request.outputPath = outputPath;
        }

    } else {
        request.handleResult = "to_pdf";
        request.main_op = "html_to_pdf";
    }
    sendMessage(request, true);
}

function initialize() {
    'use strict';
    if (!initialized) {
        initialized = true;

        $(".do_upload, .do_send, .do_fillsign, .do_export, .do_acrobat").click(
            function (e) {
                var $target = $(e.currentTarget);
                clearTimer();

                if ($target.hasClass("do_upload")) {
                    upload();
                }

                if ($target.hasClass("do_send")) {
                    send();
                }

                if ($target.hasClass("do_fillsign")) {
                    fillsign();
                }

                if ($target.hasClass("do_export")) {
                    do_export();
                }

                if ($target.hasClass("do_acrobat")) {
                    doAcrobat();
                }
            }
        );

        $(".do_visit_acom, .do_html_to_pdf").click(
            function (e) {
                var $target = $(e.currentTarget);
                clearTimer();

                if ($target.hasClass("do_visit_acom")) {
                    if (request.is_pdf) {
                        analytics(events.TREFOIL_PDF_VISIT_AIC);
                    } else {
                        analytics(events.TREFOIL_HTML_VISIT_AIC);
                    }
                    request.handleResult = "acom";
                    request.main_op = "goto_acom";
                    sendMessage(request);
                }

                if ($target.hasClass("do_html_to_pdf")) {
                    to_html();
                }
            }
        );
        $(".close-dialog").click(
            function () {
                sendCancel();
            }
        );
        $(".acrobatMainDiv").hover(clearTimer, setTimer);

        $(".sign-out").click(function () {
            analytics(acom_analytics.e.SIGN_OUT_CLICKED);
            request.main_op = "sign-out";
            sendMessage(request);
        });

        $("#special").click(function () {
            analytics(events.FLICKR_OFFER_CLICKED);
            request.main_op = "flickr";
            sendMessage(request);
        });

        $(".do_html_add_to_pdf").click(function () {
            analytics(acom_analytics.e.TREFOIL_HTML_CONVERT_APPEND);

            if (!util.getCookie("ViewResultsPref")) {
                analytics(events.TREFOIL_HTML_CONVERT_OPEN_DEFAULT);
            } else if (isChecked) {
                analytics(events.TREFOIL_HTML_CONVERT_OPEN_CHANGED);
            } else {
                analytics(events.TREFOIL_HTML_CONVERT_NO_OPEN);
            }

            request.main_op = "appendToExistingPDFPopupMenu";
            sendMessage(request);
        });

        $(".do_set_open_pref").click(function () {
            isChecked = $(".do_set_open_pref")
                .hasClass("open-pdf-in-acrobat");

            // toggle the value
            isChecked = !isChecked;
            util.setCookie(
                "ViewResultsPref",
                isChecked ? "true" : "false",
                3650
            );

            $(".do_set_open_pref").toggleClass("open-pdf-in-acrobat");
            if (isChecked) {
                analytics(acom_analytics.e.TREFOIL_HTML_OPENPDF_PREF_OFF);
            } else {
                analytics(acom_analytics.e.TREFOIL_HTML_OPENPDF_PREF_ON);
            }
            request.main_op = "send-analytics";
            sendMessage(request, true);
        });

        $(".do-acro-prefs").click(function () {
            analytics(events.TREFOIL_HTML_PREFERENCES_CLICK);
            request.main_op = "showConversionSettingsDialog";
            sendMessage(request);
        });

        $(".convert").click(function () {
            // TODO: Analytics for this action
            // make sure conversion has been enabled.
            if ($(".convert").hasClass("convert-button")) {
                request.main_op = "open_converted_file";
                sendMessage(request);
            }
        });

        $(".always-show").prop(
            "checked",
            util.getCookie("always-show-pdf-menu") !== "false"
        );

        $(".always-show").click(function () {
            var value = $(".always-show").prop("checked") ? "true" : "false";
            util.setCookie("always-show-pdf-menu", value, 3650);
        });
    }
}

function dump(msg, title) {
    'use strict';
    if (SETTINGS.DEBUG_MODE) {
        var p, o = [title];
        for (p in msg) {
            if (msg.hasOwnProperty(p)) {
                o.push("  " + p + ": " + msg[p]);
            }
        }
        console.log(o.join("\n"));
    }
}

function tester(r) {
    'use strict';
    util.consoleLog("TESTING");
    util.consoleLogDir(JSON.stringify(r));
    switch (r.test_extension) {
    case "upload":
        upload();
        break;
    case "export":
        do_export();
        break;
    case "send":
        send();
        break;
    case "fillsign":
        fillsign();
        break;
    case "to_html":
        to_html(r.outputPath);
        break;
    }
}

function setStatus(r) {
    'use strict';
    var str_status,
        complete = true,
        success = true,
        op,
        cancel = false,
        cls,
        error_title = "web2pdfStatusFailure",
        success_title = "web2pdfStatusComplete";

    if (r.test_extension) {
        return tester(r);
    }

    initialize();
    request = r;
    delete request.analytics;
    clearTimer();
    timer_off = false;

    util.translateElements(".translate");
    if (request.version === 1) {
        // if it's an older shim, we can't offer open --
        // just download (which then hopefully launches acrobat)
        $("#web2pdfOpenButtonText").val(
            util.getTranslation("web2pdfOpenButtonTextOlder")
        );
    }
    $(".ui-element").addClass("hidden");
    $("#action_message").text("");

    if (request.displayName && !SETTINGS.USE_ACROBAT) {
        $(".displayName").text(request.displayName);
        $(".sign-out").removeClass("hidden");
        $(".action-signout").removeClass("hidden");
    }

    dump(request, "Receive frame message:");

    op = request.panel_op;
    delete request.panel_op;
    switch (op) {
    case "pdf_menu":
        if (SETTINGS.USE_ACROBAT) {
            $(".acro-option.pdf").removeClass("hidden");
        } else {
            $(".api-option.pdf").removeClass("hidden");
        }
        $(".acro-option.horizontal-rule").removeClass("hidden");
        break;

    case "error":
        $(".error")
            .removeClass("hidden")
            .text("Unexpected Error:" + request.error.name + "\n" +
                  "Reference: " + request.error.errnum + "\n" +
                  request.error.details);
        break;

    case "flickr":
        $(".action-available").removeClass("hidden");
        $("#action_message")
            .text("Create\xA0slide\xA0shows\xA0and contact\xA0sheets.");
        $(".special_question").removeClass("hidden");
        $("#special").removeClass("hidden");
        break;

    case "status":
        $(".progress-area").removeClass("hidden");
        $(".convert").text(request.domtitle);
        $(".convert-status, .convert-title").addClass("hidden");
        $(".convert").removeClass("convert-button hidden");


        if (request.current_status === "waiting") {
            analytics(events.TREFOIL_HTML_CONVERT_WAITING);
            str_status = util.getTranslation("web2pdfStatusWaiting");
            complete = false;

        } else if (request.current_status === "downloading") {
            analytics(events.TREFOIL_HTML_CONVERT_DOWNLOADING);
            str_status = util.getTranslation("web2pdfStatusDownloading");
            complete = false;

        } else if (request.current_status === "in_progress") {
            analytics(events.TREFOIL_HTML_CONVERT_IN_PROGRESS);
            str_status = util.getTranslation("web2pdfStatusInProgress");
            complete = false;

        } else if (request.current_status === "filelocked") {
            str_status = util.getTranslation("web2pdfFileLockedError");

        } else if (request.current_status === "cancelled") {
            analytics(events.TREFOIL_HTML_CONVERT_CANCELLED);
            str_status = util.getTranslation("web2pdfStatusCancelled");
            cancel = true;

        } else if (request.current_status === "complete") {
            analytics(events.TREFOIL_HTML_CONVERT_COMPLETE);
            // file_path only gets populated in the latest acrobat (15)
            // That's the only time we can offer a click-to-open
            if (request.file_path) {
                $(".convert").text(
                    util.getTranslation("web2pdfOpenInDCButtonText")
                );
                $(".convert").addClass("convert-button");
            } else {
                // legacy shim behavior
                $(".convert").empty();
                $(".convert").addClass("hidden");
            }

        } else if (request.current_status === "failure") {
            analytics(events.TREFOIL_HTML_CONVERT_FAILED);
            if (request.message) {
                str_status = request.message;
            }
            success = false;

        } else if (request.current_status === "noacrobat") {
            analytics(events.TREFOIL_HTML_CONVERT_NO_ACROBAT);
            str_status = util.getTranslation(
                "web2pdfUnsupportedAcrobatVersion"
            );
            success = false;

        } else if (request.current_status === "unknown") {
            str_status = util.getTranslation("web2pdfStatusUnknownFailure");
            success = false;

        } else if (request.current_status === "pdf_downloading") {
            str_status = util.getTranslation("web2pdfStatusDownloadingPDF");
            complete = false;

        } else if (request.current_status === "pdf_failure") {
            analytics(events.TREFOIL_PDF_DOWNLOAD_FAILED);
            error_title = "web2pdfStatusUnknownFailure";
            success = false;

        } else if (request.current_status === "pdf_downloaded") {
            str_status = util.getTranslation("web2pdfPDFOpening");
            complete = false;

        } else if (request.current_status === "pdf_opened") {
            success = true;
            success_title = "web2pdfPDFOpened";

        } else if (request.current_status === "pdf_open_failed") {
            success = false;
            complete = true;
            error_title = "web2pdfPDFOpenFailed";
        }
        // use .html because messages may have markup -- and we trust them
        if (str_status) {
            $(".convert-title").removeClass("hidden");
            $(".convert-title").html(str_status);
        }

        if (complete) {
            delete request.panel_op;

            $(".actions").removeClass("hidden");
            if (SETTINGS.USE_ACROBAT) {
                cls = request.is_pdf ? ".acro-option.pdf" : ".acro-option.html";
            } else {
                cls = request.is_pdf ? ".api-option.pdf" : ".api-option.html";
            }
            $(cls).removeClass("hidden");

            $(".convert").removeClass("convert-busy");
            $(".convert-status").removeClass("hidden");

            if (success) {
                $(".convert-status-icon").addClass("icon-success");
                $(".convert-status-title").text(
                    util.getTranslation(success_title)
                );
            } else {
                // for errrors, keep the dialog in place
                timer_off = true;
                $(".convert-status-icon").removeClass("icon-success");
                $(".convert-status-icon").addClass("icon-error");
                $(".convert-status-title").text(
                    util.getTranslation(error_title)
                );
                $(".convert").addClass("hidden");
            }
            if (cancel) {
                $(".convert-status").addClass("hidden");
                $(".convert").addClass("hidden");
            }

        } else {
            timer_off = true;
            $(".actions").addClass("hidden");
            $(".convert").addClass("convert-busy");
        }
        break;

    case "html_menu":
        if (SETTINGS.USE_ACROBAT) {
            $(".acro-option.html").removeClass("hidden");
        } else {
            $(".api-option.html").removeClass("hidden");
        }
        break;
    }
    setTimer();
}

if (util.isChrome()) {
    $(function () {
        'use strict';
        initialize();
    });
}

util.addMainListener(setStatus);

$(function () {
    'use strict';
    // grab the initial message from the URL parameter
    if (window.location.search) {
        request = JSON.parse(
            decodeURIComponent(
                window.location.search.split("=")[1]
            )
        );
        if (SETTINGS.TEST_MODE) {
            // in test mode, we'll accept messages using postMessage() so that
            // we can send messages from webdriver script.
            window.addEventListener("message", function (e) {
                setStatus(e.data);
            }, false);
        }

        setStatus(request);
        if (util.getCookie("ViewResultsPref") === "false") {
            isChecked = false;
        } else {
            $(".do_set_open_pref").addClass("open-pdf-in-acrobat");
            isChecked = true;
        }
    }
});
