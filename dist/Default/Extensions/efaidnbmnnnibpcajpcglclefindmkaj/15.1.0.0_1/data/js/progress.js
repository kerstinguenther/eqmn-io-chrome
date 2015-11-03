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

/*global $, console, util, addon, self, SETTINGS, acom_analytics */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

$(function () {
    'use strict';

    var doneHandler = function () {
        util.messageToMain(
            {
                main_op: "file_spec_done",
                dest_folder: $(".selected").parent().data("id"),
                filename: $(".filename").val()
            }
        );
    },
        params, param, filename, blobPromise = $.Deferred();

    function selectFolder($folder) {
        $(".selected").removeClass("selected");
        $folder.children(".folder-display").addClass("selected");
    }

    function rootFolderOnly() {
        $(".nofolders-message").removeClass("hidden");
    }

    function renderFolders(request) {
        var $folder = $(".folder").detach(),
            root,
            INDENT = 20,    // px
            $root;

        function renderFolder(name, id, $parent, indent) {
            var $f = $folder
                        .clone()
                        .data("id", id)
                        .css("margin-left", indent + "px")
                        // so our test scripts can find it
                        .addClass("folder-found")
                        .appendTo($parent);

            $f.find(".folder-name").text(name || "root");
            return $f;
        }

        function addFolder(folders, parent, $context, offset) {
            var list = $.grep(
                folders,
                function (f) {
                    return f.parent_id === parent;
                }
            );
            $.each(list, function (index, folder) {
                var $f = renderFolder(folder.name, folder.id, $context, offset);

                addFolder(folders, folder.id, $f, offset + INDENT);
            });
        }
        root = $.grep(
            request.folders,
            function (f) {
                return f.object_type === "root";
            }
        )[0];
        $root = renderFolder(root.name, root.id, $(".folders"), 0);
        addFolder(request.folders, root.id, $root, INDENT);
        selectFolder($root);
        if (request.folders.length === 1) {
            rootFolderOnly();
        }
    }

    function setTitle(title) {
        $(".header-text")
            .text(title)
            .removeClass("hidden");
    }

    function setBusy(busy) {
        if (busy) {
            $(".progress").removeClass("hidden");
        } else {
            $(".progress").addClass("hidden");
        }
    }

    function setError(msg) {
        setBusy(false);
        $("#error")
            .removeClass("hidden")
            .text(msg);

        $(".content, .footer").addClass("hidden");
    }

    function analytics(str) {
        acom_analytics.event(str);
    }

    function handler(msg) {
        util.consoleLog("handler message: ");
        //util.consoleLogDir(msg);
        if (msg.progress_op === "set-text") {
            setTitle(msg.text);
            setBusy(msg.busy);
        }
        if (msg.progress_op === "set-error") {
            setError(msg.text);
        }
        if (msg.progress_op === "folders") {
            renderFolders(msg);
        }

        if (msg.progress_op === "html-blob") {
            util.consoleLog(msg.blob.html.length);
            util.consoleLog("received html blob");
            setBusy(false);
            if (msg.error) {
                setError(msg.error);
                setTitle("Oops!");
                util.consoleLog(msg.error);
                analytics(acom_analytics.e.HTML_SOURCE_SIZE_TOO_LARGE_ERROR);
            } else {
                // uncomment this to see the contents of the html
                setTitle("HTML Ready for Upload and Conversion");
                // Natia:
                // For debugging purposes, we'll keep the logic behind
                // this promise object (the post to the local server)
                // but the main processing will happen from session.js
                blobPromise.resolve(msg.blob);

                //log analytics
                var blobSize = msg.blob.currentSize / 1048576; //convert to MB
                acom_analytics.checkAndLogHTMLBlobSize(blobSize);
            }
        }
    }

    $(".translate").each(function () {
        util.translateElement(this);
    });

    function decodeParam(val) {
        return decodeURIComponent(val.replace(/\+/g, " "));
    }

    if (location.hash.length > 1) {
        params = location.hash.substring(1).split("&");

        $.each(params, function () {
            param = this.split("=");

            if (param[0] === "filename") {
                filename = decodeParam(param[1]);
                $(".fileicon")
                    .removeClass("hidden");
                $(".done").removeClass("hidden");
                $(".filelabel")
                    .text(filename)
                    .removeClass("hidden");
                $(".filename")
                    .val(filename)
                    .removeClass("hidden");

                $(".folders").removeClass("hidden");
                setBusy(false);
            }

            if (param[0] === "message") {
                setTitle(decodeParam(param[1]));
            }

            if (param[0] === "busy") {
                setBusy(param[1] === "true");
            }
            //Use local node server only if USE_ECHO_SERVICE is set to true
            if (SETTINGS.USE_ECHO_SERVICE) {
                if (param[0] === "progress_op" && param[1] === "htmlToPdf") {
                    doneHandler = function () {
                        blobPromise.then(function (data) {
                            // send the blob to our local echoservice
                            $.ajax("http://127.0.0.1:1234/", {
                                type: "POST",
                                data: JSON.stringify(data),
                                contentType: "application/json"
                            }).then(
                                function (data) {
                                    self.close();
                                },
                                function () {
                                    setError(
                                        "To see intermediate results," +
                                            " run the local echo service"
                                    );
                                }
                            );
                        });
                    };
                }
            }
            if (param[0] === "unavailable") {
                setTitle("Oops!");
                if (param[1] === "html_to_pdf") {
                    $("#message")
                        .addClass("error")
                        .text("HTML to PDF not yet available."  +
                              "We know you want it.  " +
                              "We want it too. " +
                              "Please give us a few months to work on it.")
                        .removeClass("hidden");

                    $(".right-content, .progress").addClass("hidden");
                }
                if (param[1] === "flickr") {
                    $("#message")
                        .addClass("error")
                        .text("Flickr apps not yet available.")
                        .removeClass("hidden");
                    $(".right-content, .progress").addClass("hidden");
                }

            }
        });
        window.location.hash = "";
    }

    $(".folders").click(
        function (e) {
            if ($(e.target).is(".folder-display *")) {
                selectFolder($(e.target).parent().parent());
            }
        }
    );

    function done() {
        doneHandler();
    }

    $(".done").click(
        function () {
            var filename = $(".filename").val();
            $(".fileicon")
                .removeClass("hidden");
            $(".filelabel")
                .text(filename)
                .removeClass("hidden");

            $(".done, .right-content").addClass("hidden");
            setBusy(true);
            done();
        }
    );
    util.addMainListener(handler);
});

