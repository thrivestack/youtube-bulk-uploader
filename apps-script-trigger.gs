// Note: Apps Script automatically requests authorization
// based on the API's used in the code.

const CHANNEL_VIDEOS_SHEET_NAME = "Channel Videos List";
const CHANNEL_VIDEOS_SHEET_HEADER = ["Video Title", "Video ID", "Video Description", "Thumbnail", "Video URL"];
const FILE_UPLOAD_LIST_SHEET_NAME = "File Upload List";
const FILE_UPLOAD_LIST_SHEET_HEADER = ["File Name", "File ID", "Title", "Description", "Tags", "Self Declared Made For Kids", "YouTube URL"];
const CLOUD_FUNCTION_URL = 'https://us-east1-thrive-alo.cloudfunctions.net/youtube-bulk-uploader';
const ONBOARDING_FUNCTION_URL = 'https://us-east1-thrive-alo.cloudfunctions.net/youtube-bulk-uploader-onboarding';

function getConfigValues_() {
  const ss = SpreadsheetApp.getActive();
  return {
    channelId: String(ss.getRange("Config!B3").getValue() || '').trim(),
    driveFolderId: String(ss.getRange("Config!B4").getValue() || '').trim(),
    completedFolderId: String(ss.getRange("Config!B6").getValue() || '').trim(),
    defaultDescription: String(ss.getRange("Config!B2").getValue() || ''),
    postUploadAction: String(ss.getRange("Config!B5").getValue() || '').trim()
  };
}


function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('YouTube Bulk Uploader')

    .addItem('List Videos From Google Drive', 'pullFilesFromRootFolder')
    .addSeparator()
    .addItem('Upload To Youtube', 'triggerUpload')
    .addSeparator()
    .addItem('Client Onboarding (One Click)', 'runClientOnboarding')
    .addSeparator()
    .addItem('List Uploaded Videos', 'listMyUploads')
    .addToUi();
}

function callCloudFunction_(url, payload) {
  var token = ScriptApp.getIdentityToken();
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    headers: {
      Authorization: 'Bearer ' + token
    }
  };
  return UrlFetchApp.fetch(url, options);
}

function listMyUploads() {
  Logger.log("inside listMyUploads")
  var ui = SpreadsheetApp.getUi();
  var cfg = getConfigValues_();
  var channelID = cfg.channelId;
  var sheet = SpreadsheetApp.getActive().getSheetByName(CHANNEL_VIDEOS_SHEET_NAME);
  var valuesForSheet = []; //File array to be written to sheet in the end
  valuesForSheet.push(CHANNEL_VIDEOS_SHEET_HEADER);

  try {
    var channelParams = channelID ? { id: channelID } : { mine: true };
    var results = YouTube.Channels.list('contentDetails,snippet', channelParams);

    if (!results.items || results.items.length === 0) {
      ui.alert(
        'Channel not found',
        'No YouTube channel was found for Config!B3. Use the real Channel ID (starts with "UC"), not @handle or channel URL.',
        ui.ButtonSet.OK
      );
      return;
    }

    var targetChannelId = channelID || results.items[0].id;
    var nextPageToken = '';
    while (nextPageToken != null) {
      var searchResponse = YouTube.Search.list('id,snippet', {
        channelId: targetChannelId,
        maxResults: 50,
        order: 'date',
        type: 'video',
        pageToken: nextPageToken
      });

      var searchItems = searchResponse.items || [];
      for (var i = 0; i < searchItems.length; i++) {
        var searchItem = searchItems[i];
        if (!searchItem.id || !searchItem.id.videoId) {
          continue;
        }

        valuesForSheet.push([
          searchItem.snippet.title,
          searchItem.id.videoId,
          searchItem.snippet.description,
          "=IMAGE(\"" + searchItem.snippet.thumbnails.default.url + "\")",
          "https://www.youtube.com/watch?v=" + searchItem.id.videoId
        ]);
      }
      nextPageToken = searchResponse.nextPageToken;
    }

    sheet.clearContents();
    var lastRow = sheet.getLastRow();
    var range = sheet.getRange(lastRow + 1, 1, valuesForSheet.length, 5);
    range.setValues(valuesForSheet);
  } catch (error) {
    Logger.log('listMyUploads error: ' + error);
    ui.alert(
      'Failed to list uploads',
      'YouTube returned: ' + error.message + '\n\nMost common cause: Config!B3 is not a valid Channel ID for an accessible channel. Use a channel ID that starts with "UC".',
      ui.ButtonSet.OK
    );
  }
}

function triggerUpload() {
  const ui = SpreadsheetApp.getUi();
  const cfg = getConfigValues_();

  if (!cfg.driveFolderId || !cfg.channelId) {
    ui.alert(
      '❌ Missing configuration',
      'Please ensure Config!B3 (YouTube Channel ID) and Config!B4 (Drive Root Folder ID) are filled in.',
      ui.ButtonSet.OK
    );
    return;
  }

  // Confirm action
  const response = ui.alert(
    'YouTube videos upload',
    'This will start uploading videos from your Google Drive folder to YouTube.\n\n' +
    'Continue?',
    ui.ButtonSet.YES_NO
  );

  if (response !== ui.Button.YES) {
    return;
  }

  // Show progress
  const progressMsg = ui.alert(
    '⏳ Processing...',
    'Uploading video. This may take a few minutes to complete...',
    ui.ButtonSet.OK
  );

  try {
    // Prepare request payload
    const payload = {
      drive_root_folder_id: cfg.driveFolderId,
      youtube_channel_id: cfg.channelId,
      default_video_description: cfg.defaultDescription,
      post_upload_action: cfg.postUploadAction,
      completed_folder_id: cfg.completedFolderId
    };

    Logger.log('Triggering Cloud Function: ' + CLOUD_FUNCTION_URL);
    Logger.log('Payload: ' + JSON.stringify(payload));

    var httpResponse = callCloudFunction_(CLOUD_FUNCTION_URL, payload);
    var statusCode = httpResponse.getResponseCode();
    var responseText = httpResponse.getContentText();

    Logger.log('Response Status: ' + statusCode);
    Logger.log('Response Body: ' + responseText);

    // Handle response
    if (statusCode === 200) {
      var resultMessage = '🥳 Upload was success!\n\n';

      try {
        var result = JSON.parse(responseText);
        if (result.result) {
          resultMessage += 'Result: ' + result.result + '\n\n';
        }
        resultMessage += 'Check the "Logs" sheet for details.';
      } catch (e) {
        resultMessage += 'Response: ' + responseText;
      }

      ui.alert('Success', resultMessage, ui.ButtonSet.OK);

    } else {
      ui.alert(
        '❌ Error',
        'Failed to upload.\n\n' +
        'Status Code: ' + statusCode + '\n' +
        'Response: ' + responseText.substring(0, 500),
        ui.ButtonSet.OK
      );
    }

  } catch (error) {
    Logger.log('Error triggering function: ' + error);
    ui.alert(
      '❌ Error',
      'Failed to trigger the upload function.\n\n' +
      'Error: ' + error.message + '\n\n' +
      'Please contact Thrive Digital',
      ui.ButtonSet.OK
    );
  }
}

function _openAuthPopup_(authUrl) {
  var html = HtmlService.createHtmlOutput(
    '<div style="font-family:Arial,sans-serif;padding:12px;">' +
    '<p>Opening Google consent screen...</p>' +
    '<p>If no new tab appears, allow popups and <a href="' + authUrl + '" target="_blank">click here</a>.</p>' +
    '<script>window.open(' + JSON.stringify(authUrl) + ', "_blank");google.script.host.close();</script>' +
    '</div>'
  ).setWidth(420).setHeight(160);
  SpreadsheetApp.getUi().showModalDialog(html, 'Client Onboarding');
}

function runClientOnboarding() {
  var ui = SpreadsheetApp.getUi();
  var cfg = getConfigValues_();

  if (!cfg.channelId || !cfg.driveFolderId) {
    ui.alert(
      '❌ Missing configuration',
      'Please set Config!B3 (YouTube Channel ID) and Config!B4 (Drive Root Folder ID) before onboarding.',
      ui.ButtonSet.OK
    );
    return;
  }

  var redeployPrompt = ui.alert(
    'Redeploy uploader after onboarding?',
    'Choose YES to request redeploy after secret rotation. Choose NO to skip redeploy and continue immediately.',
    ui.ButtonSet.YES_NO
  );
  var redeployRequested = redeployPrompt === ui.Button.YES;

  try {
    var payload = {
      action: 'start_onboarding',
      youtube_channel_id: cfg.channelId,
      redeploy: redeployRequested,
      trigger_upload: true,
      upload_function_url: CLOUD_FUNCTION_URL,
      upload_payload: {
        drive_root_folder_id: cfg.driveFolderId,
        youtube_channel_id: cfg.channelId,
        default_video_description: cfg.defaultDescription,
        post_upload_action: cfg.postUploadAction,
        completed_folder_id: cfg.completedFolderId
      }
    };

    var response = callCloudFunction_(ONBOARDING_FUNCTION_URL, payload);
    var statusCode = response.getResponseCode();
    var body = response.getContentText();

    if (statusCode !== 200) {
      ui.alert('Onboarding error', 'Status: ' + statusCode + '\n' + body.substring(0, 1200), ui.ButtonSet.OK);
      return;
    }

    var data = JSON.parse(body);
    var authUrl = data.auth_url || '';
    if (!authUrl) {
      ui.alert('Onboarding error', 'No auth_url returned.\n' + body.substring(0, 1200), ui.ButtonSet.OK);
      return;
    }

    _openAuthPopup_(authUrl);
    ui.alert(
      'Onboarding started',
      'Complete the Google consent screen in the opened tab. Once approved, onboarding and upload continue automatically in the background.',
      ui.ButtonSet.OK
    );
  } catch (error) {
    Logger.log('runClientOnboarding error: ' + error);
    ui.alert('Onboarding error', 'Failed to start onboarding.\n' + error.message, ui.ButtonSet.OK);
  }
}

function completeClientOnboarding() {
  var ui = SpreadsheetApp.getUi();
  ui.alert(
    'Deprecated step',
    'Manual completion is no longer required. Use "Client Onboarding (One Click)" and complete OAuth consent in the popup tab.',
    ui.ButtonSet.OK
  );
}

function traverseSubFolders(parent, list) {
  parent = parent.getId();
  var childFolder = DriveApp.getFolderById(parent).getFolders();
  while(childFolder.hasNext()) {
    var child = childFolder.next();
    addFilesToList(child, list);
    //Logger.log(child.getName() + " " + child.getId());
    traverseSubFolders(child, list);
  }
  return;
}

function addFilesToList(fromFolder, list) {
  var files = fromFolder.getFiles();
    while (files.hasNext()) {
    var file = files.next();
    var fileName = file.getName().toLowerCase();
    if(fileName.indexOf(".mp4") > -1 || fileName.indexOf(".mov") > -1 || fileName.indexOf(".avi") > -1) //if file is a video
      list.push([fromFolder.getName(), fromFolder.getId(), file.getName(), file.getId()]);
  }
}

function pullFilesFromRootFolder()
{
  var folderList = [];
  var rootFolderId = SpreadsheetApp.getActive().getRange("Config!B4").getValue();
  folderList = traverseDriveFolderforSubFolders(rootFolderId, folderList);
  folderList.push(rootFolderId);

  var searchString = "(parents in '";
  var sheet = SpreadsheetApp.getActive().getSheetByName("File Upload List");
  var valuesForSheet = []; //File array to be written to sheet in the end
  valuesForSheet.push(FILE_UPLOAD_LIST_SHEET_HEADER);

  for(var i in folderList){
    var folderID = folderList[i];
    Logger.log("folderID: " + folderID);
    if(i == folderList.length-1){
      searchString = searchString + folderID;
    }
    else {
      searchString = searchString + folderID + "' or parents in '";
    }
  }

  searchString = searchString + "') and (title contains '.mp4' or title contains '.mov' or title contains '.avi')";

  Logger.log(searchString);
  var files = DriveApp.searchFiles(searchString);

  while (files.hasNext()) {
    var file = files.next();
    valuesForSheet.push([file.getName(), file.getId(), "", "", "", "", ""]);
  }

  sheet.clearContents();
  var lastRow = sheet.getLastRow();
  var range = sheet.getRange(lastRow + 1, 1, valuesForSheet.length, 7);
  range.setValues(valuesForSheet);
}

function traverseDriveFolderforSubFolders (rootFolderId, folderList) {
  var parentFolder = DriveApp.getFolderById(rootFolderId);
  var childFolders = parentFolder.getFolders();
  while(childFolders.hasNext()) {
    var child = childFolders.next();
    folderList.push(child.getId());
    folderList = traverseDriveFolderforSubFolders(child.getId(), folderList);
  }

  return folderList;
}
