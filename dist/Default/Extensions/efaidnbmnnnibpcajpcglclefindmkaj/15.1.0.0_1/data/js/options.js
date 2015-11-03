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

/*global console, util, chrome, $, SETTINGS */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

$(function () {
    'use strict';

    $(document).ready(function () {
        //if options page is directly opened from chrome://extensions
        util.translateElements(".translate");

        var anl,
            env,
            os,
            queryParams = document.location.search.replace(/\?/, "").split('&');

        queryParams.forEach(function (p) {
            var pr = p.split('=');
            if (pr[0] === "env") {
                env = pr[1];

            } else if (pr[0] === "anl") {
                anl = pr[1];

            } else if (pr[0] === "os") {
                os = pr[1];
            }
        });
        if (os === "mac") {
            $('.content').remove();
            $("#web2pdfMissingMac").removeClass("hidden");
            return;
        }
        if (env === "prod" || SETTINGS.USE_ACROBAT) {
            $('.choose-env').remove();

        } else if (env !== "prod") {
            $('#environment').val(env);
        }
        $(".analytics").prop("checked", anl === "true");
    });

    $("#web2pdfSave").click(function () {
        if ($("#web2pdfSave").hasClass("no_change")) {
            return;
        }
        var env = $("#environment").val(),
            html2pdf = $("#html2pdf").is(":checked"),
            msg,
            anl = $(".analytics").prop("checked");

        util.messageToMain(
            {
                main_op: "reset",
                environment: env,
                html2pdf: env === "local-dev" ? html2pdf : false,
                analytics_on: anl ? true : false
            }
        );
        if (!SETTINGS.USE_ACROBAT) {
            msg = "Success! Environment reset to " + env + ".";
            if (env === "local-dev") {
                msg += " Html2PDF set to " + html2pdf + ".";
            }
            $("#message").text(msg);
            $("#message").removeClass("hidden");
        }
    });

    $("#environment").change(function () {
        var env = $("#environment").val();
        if (env === "local-dev") {
            $(".html2pdfoption").removeClass("hidden");
        } else {
            $(".html2pdfoption").addClass("hidden");
        }
    });

    $(".analytics").click(function () {
        $("#web2pdfSave").removeClass("no_change");
    });

    util.addMainListener(function (request) {
        if (request.options_op === "saved_analytics") {
            $("#web2pdfSave").addClass("no_change");
        }
    });
});
