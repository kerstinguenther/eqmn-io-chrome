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

/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */
/*global $, chrome, console */

var $iframe, disableKeyZoom, disableWheelZoom;

function sendMsg(msg) {
    'use strict';
    chrome.runtime.sendMessage(msg);
}

function injectFrame(message) {
    'use strict';

    // send the first message as a command line parameter
    var params = "message=" + encodeURIComponent(JSON.stringify(message));

    $iframe = $("#__acrobatDialog__");

    if ($iframe.length === 0) {
        $iframe = $("<iframe>")
            .attr("id", "__acrobatDialog__")
            .css({
                border:     "0px",
                "z-index":  2147483647,
                position:   "fixed",
                top:        "-5px",
                right:      "80px",
                width:      "265px",
                height:     "450px",
                display:    "block",
                margin:     "auto"
            })
            .attr(
                "src",
                chrome.extension.getURL('data/js/frame.html') + "?" + params
            )
            .appendTo("html");

        $(document).on("keydown", disableKeyZoom);
        $(document).on('mousewheel', disableWheelZoom);

    } else if (message.trefoilClick) {
        // iframe exists and the user has clicked on the trefoil...
        // this means delete the iframe.
        $(document).off("keydown", disableKeyZoom);
        $(document).off('mousewheel', disableWheelZoom);

        delete message.trefoilClick;
        $iframe.remove();
    }
}

disableKeyZoom = function (evt) {
    'use strict';
    if (evt.ctrlKey &&
            [187, 189, 107, 109].indexOf(evt.keyCode) !== -1
            ) {
        evt.preventDefault();
    }
};

disableWheelZoom = function (evt) {
    'use strict';
    if (evt.ctrlKey) {
        evt.preventDefault();
    }
};

function handler(message) {
    'use strict';

    if (message.content_op === "dismiss") {
        delete message.content_op;
        if ($iframe) {
            $(document).off("keydown", disableKeyZoom);
            $(document).off('mousewheel', disableWheelZoom);
            $iframe.remove();
            $iframe = null;
            return;
        }
    }
    if (message.panel_op) {
        // a message for the panel.  Make sure it exists
        injectFrame(message);
    }
    return false;
}

chrome.runtime.onMessage.addListener(handler);

//when document is ready send message to enable extension icon
$(document).ready(function (msg) {
    'use strict';
    // Let the extension know that content is ready.
    // while we're at it, let the extension know if it's a pdf
    var $embed = $("embed");
    if ($embed.length > 0 && $embed.attr("type") === "application/pdf") {
        sendMsg({
            main_op     : "pdf-menu",
            is_pdf      : true,
            url         : document.location.href
        });
    } else {
        // this path is needed only for the rare case where the browser has
        // started up with an active page
        sendMsg({
            main_op     : "html-startup",
            url         : document.location.href
        });
    }
});
