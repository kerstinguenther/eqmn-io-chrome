# eqmn-io-chrome - EQMN modeler based on bpmn-io-chrome

[eqmn-io-chrome](https://github.com/kerstinguenther/eqmn-io-chrome) is an EQMN modeling tool as Google Chrome App based on [bpmn-io-chrome](https://github.com/bpmn-io/bpmn-io-chrome).


## Usage

#### Start from command line

Navigate into eqmn-io-chrome folder and type:

```bash
chrome --disable-web-security --load-and-launch-app="$(pwd)/dist"
```

__Hint:__ The option `--disable-web-security` is needed to request the event hierarchy from a web service. If you do not want to validate the model against an event hierarchy, you can leave out this option.

__Hint:__ Instead of Google Chrome you can also use the Chromium browser. Just type `chromium-browser` instead of `chrome`.


#### Manually load and start as Google Chrome App

Open Google Chrome Browser (or Chromium) and go to Settings > Extensions. Enable 'Developer Mode' and click on 'Load unpacked extension'. Navigate to 'dist' folder and click 'OK'. 

#### Create executable

Once loaded as Google Chrome App, you can lookup the extension's id in the Chrome browser under Settings > Extensions and you can use this <id> to build an executable for the app.

###### Windows

Create a `eqmn-io.bat` file, i.e. on your desktop:

```plain
chrome --disable-web-security --app-id=<id>
```

__Hint:__ If Chrome is not in your PATH, you have to type the complete path to Google Chrome, like "C:\Program Files\Google\Chrome\Application\chrome.exe".

Now just double-click bat-file to open app.

#### Linux / Mac OS X

Create an `eqmn-io` executable, i.e. under `/usr/local/bin`:

```bash
#!/bin/bash
chrome --disable-web-security --app-id=<id>
```

Execute it as `eqmn-io`.

## How to

More information about how to model EQMN models are included in the tool itself. Just click on the question mark in the menu bar.


## License

Use under the terms of the [bpmn-js license](http://bpmn.io/license).
