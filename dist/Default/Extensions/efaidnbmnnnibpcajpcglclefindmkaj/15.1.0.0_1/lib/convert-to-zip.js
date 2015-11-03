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

/*global console, zip, saveAs, require:true, exports:true, define:true, $, def:true */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("util"),
        require("common"),
        require("analytics"),
        require("proxy"),
        require("communicate")
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
    analytics,
    proxy,
    communicate
  
) {
    'use strict';
    var singleton = null, ConvertToZip, prop, upload, dwnload;
    
    upload = function () {
        return communicate.getModule("upload");
    };
    dwnload = function () {
        return communicate.getModule("download-manager");
    };
    
    ConvertToZip = function () {
        var jblob, zipWriter, reader;
        this.proxy = proxy.proxy.bind(this);
        //path for zip scripts
        zip.workerScriptsPath = "/lib/libs/";
        
        
        this.compress = function (blob, sessionRequest) {
            var writer = new zip.BlobWriter();
            jblob = JSON.parse(blob);
           
            zip.createWriter(writer, function (writer) {
                var i = 0, ref;
                zipWriter = writer;
          
                function close() {
                    zipWriter.close(function (blob) {
                        util.consoleLog("closing writer");
                        //start zip upload process once we have whole blob
                        upload().uploadHTML(blob, sessionRequest)
                            .done(proxy.proxy(dwnload().uploadHandler));
                        //saves to local hdd, for testing purposes
                        //need to comment it later
                        saveAs(blob, "test.zip");
                        zipWriter = null;
                    });
                }
                
                function nextFile(i) {
                    ref = jblob.refs[i];
                    //check if it has no ref files
                    if (!ref) {
                        close();
                    } else if (!ref.data) {
                        util.consoleLog("empty data");
                        util.consoleLogDir(ref);
                        if (i === jblob.refs.length) {
                            close();
                        } else {
                            i += 1;
                            nextFile(i);
                        }
                    } else {
                        var ftype, assetName;
                        reader = new zip.TextReader(ref.data);
                        if (ref.type === "html") {
                            ftype = "text/html";
                            assetName = ref.placeholder;
                        } else if (ref.type === "css") {
                            assetName = "refs/" +
                                ref.placeholder.replace(/refs\//, "");
                            ftype = "text/css";
                        } else if (ref.type === "image") {
                            assetName = "refs/" +
                                ref.placeholder.replace(/refs\//, "");
                            ftype = "image/jpeg";
                            reader = new zip.Data64URIReader(ref.data);
                        } else {
                            ftype = "text/plain";
                            assetName = "refs/" +
                                ref.placeholder.replace(/refs\//, "");
                        }

                        //add the ref file to zip
                        zipWriter.add(assetName, reader, function () {
                            util.consoleLog("Added ref file");
                            i += 1;
                            if (i < jblob.refs.length) {
                                nextFile(i);
                            } else {
                                close();
                            }
                        });
                    }
                }
                
                zipWriter.add("index.html",
                              new zip.TextReader(jblob.html), function () {
                        util.consoleLog("Added main html file");
                        nextFile(i);
                    });
            }, function () {
                util.consoleLog("Error occured while creating zip writer.");
            });
        };
    };
    
    if (!singleton) {
        singleton = new ConvertToZip();
        communicate.registerModule("convert-to-zip", singleton);
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
