/**
 * YouTube Bulk Uploader - Apps Script Trigger
 * 
 * This script allows you to manually trigger the YouTube Bulk Uploader
 * Cloud Function from your Google Spreadsheet.
 * 
 * Setup Instructions:
 * 1. Open your Google Spreadsheet
 * 2. Go to Extensions > Apps Script
 * 3. Replace the default code with this script
 * 4. Update the CLOUD_FUNCTION_URL constant below with your function URL
 * 5. Save and refresh your spreadsheet
 * 6. You'll see a new menu "YouTube Uploader" with options to trigger uploads
 */

// ============================================================================
// CONFIGURATION - Update this with your Cloud Function URL
// ============================================================================
const CLOUD_FUNCTION_URL = 'YOUR_CLOUD_FUNCTION_URL_HERE';
// Example: 'https://us-east1-your-project.cloudfunctions.net/youtube-bulk-uploader'

// Get config values from the spreadsheet
const SPREADSHEET = SpreadsheetApp.getActive();
const DRIVE_FOLDER_ID = '1cMlIsNLn7KoaMjHEZYjn8pavtQF43xAM';
const YOUTUBE_CHANNEL_ID = 'UCDhxapLoA0eGl7PJa4ZtTiw';

// ============================================================================
// MENU FUNCTIONS
// ============================================================================

/**
 * Creates a custom menu in the spreadsheet when opened
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('YouTube Uploader')
      .addItem('🚀 Upload Videos Now', 'triggerUpload')
      .addSeparator()
      .addItem('📊 Check Function Status', 'checkFunctionStatus')
      .addItem('⚙️ Configure Function URL', 'promptForFunctionUrl')
      .addSeparator()
      .addItem('ℹ️ Help', 'showHelp')
      .addToUi();
}

// ============================================================================
// MAIN TRIGGER FUNCTION
// ============================================================================

/**
 * Triggers the YouTube Bulk Uploader Cloud Function
 */
function triggerUpload() {
  const ui = SpreadsheetApp.getUi();
  
  // Validate configuration
  if (!CLOUD_FUNCTION_URL || CLOUD_FUNCTION_URL === 'YOUR_CLOUD_FUNCTION_URL_HERE') {
    ui.alert(
      '⚠️ Configuration Required',
      'Please configure the Cloud Function URL first.\n\n' +
      'Go to: YouTube Uploader > Configure Function URL',
      ui.ButtonSet.OK
    );
    return;
  }
  
  // Confirm action
  const response = ui.alert(
    '🚀 Trigger YouTube Upload',
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
    'Triggering the upload function. This may take a few moments...',
    ui.ButtonSet.OK
  );
  
  try {
    // Prepare request payload
    const payload = {
      drive_root_folder_id: DRIVE_FOLDER_ID,
      youtube_channel_id: YOUTUBE_CHANNEL_ID,
      spreadsheet_id: SPREADSHEET.getId()
    };
    
    // Make HTTP request to Cloud Function
    const options = {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    };
    
    Logger.log('Triggering Cloud Function: ' + CLOUD_FUNCTION_URL);
    Logger.log('Payload: ' + JSON.stringify(payload));
    
    const httpResponse = UrlFetchApp.fetch(CLOUD_FUNCTION_URL, options);
    const statusCode = httpResponse.getResponseCode();
    const responseText = httpResponse.getContentText();
    
    Logger.log('Response Status: ' + statusCode);
    Logger.log('Response Body: ' + responseText);
    
    // Handle response
    if (statusCode === 200) {
      let resultMessage = '✅ Upload triggered successfully!\n\n';
      
      try {
        const result = JSON.parse(responseText);
        if (result.result) {
          resultMessage += 'Result: ' + result.result + '\n\n';
        }
        resultMessage += 'Check the "Logs" sheet for details.';
      } catch (e) {
        resultMessage += 'Response: ' + responseText;
      }
      
      ui.alert('Success', resultMessage, ui.ButtonSet.OK);
      
      // Refresh the logs sheet if it exists
      refreshLogsSheet();
      
    } else {
      ui.alert(
        '❌ Error',
        'Failed to trigger upload.\n\n' +
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
      'Please check:\n' +
      '1. Cloud Function URL is correct\n' +
      '2. Function is deployed and accessible\n' +
      '3. You have internet connectivity',
      ui.ButtonSet.OK
    );
  }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Checks if the Cloud Function is accessible
 */
function checkFunctionStatus() {
  const ui = SpreadsheetApp.getUi();
  
  if (!CLOUD_FUNCTION_URL || CLOUD_FUNCTION_URL === 'YOUR_CLOUD_FUNCTION_URL_HERE') {
    ui.alert(
      '⚠️ Not Configured',
      'Cloud Function URL is not configured.',
      ui.ButtonSet.OK
    );
    return;
  }
  
  try {
    const options = {
      method: 'get',
      muteHttpExceptions: true
    };
    
    const response = UrlFetchApp.fetch(CLOUD_FUNCTION_URL, options);
    const statusCode = response.getResponseCode();
    
    if (statusCode === 200 || statusCode === 405) {
      // 405 is okay - means function exists but doesn't accept GET
      ui.alert(
        '✅ Function is Accessible',
        'The Cloud Function is deployed and accessible.\n\n' +
        'URL: ' + CLOUD_FUNCTION_URL + '\n' +
        'Status: Online',
        ui.ButtonSet.OK
      );
    } else {
      ui.alert(
        '⚠️ Unexpected Response',
        'Status Code: ' + statusCode + '\n\n' +
        'The function may not be configured correctly.',
        ui.ButtonSet.OK
      );
    }
  } catch (error) {
    ui.alert(
      '❌ Function Not Accessible',
      'Could not reach the Cloud Function.\n\n' +
      'Error: ' + error.message + '\n\n' +
      'Please verify:\n' +
      '1. The function is deployed\n' +
      '2. The URL is correct\n' +
      '3. The function allows unauthenticated access',
      ui.ButtonSet.OK
    );
  }
}

/**
 * Prompts user to configure the Cloud Function URL
 */
function promptForFunctionUrl() {
  const ui = SpreadsheetApp.getUi();
  
  const result = ui.prompt(
    '⚙️ Configure Cloud Function URL',
    'Enter your Cloud Function URL:\n\n' +
    'Example:\nhttps://us-east1-your-project.cloudfunctions.net/youtube-bulk-uploader\n\n' +
    'Current URL: ' + CLOUD_FUNCTION_URL,
    ui.ButtonSet.OK_CANCEL
  );
  
  if (result.getSelectedButton() === ui.Button.OK) {
    const newUrl = result.getResponseText().trim();
    
    if (newUrl && newUrl.startsWith('http')) {
      // Store in Script Properties
      PropertiesService.getScriptProperties().setProperty('FUNCTION_URL', newUrl);
      
      ui.alert(
        '✅ Saved',
        'Cloud Function URL has been configured.\n\n' +
        'Note: For permanent configuration, update the CLOUD_FUNCTION_URL\n' +
        'constant in the Apps Script code.',
        ui.ButtonSet.OK
      );
    } else {
      ui.alert(
        '❌ Invalid URL',
        'Please enter a valid HTTP/HTTPS URL.',
        ui.ButtonSet.OK
      );
    }
  }
}

/**
 * Shows help information
 */
function showHelp() {
  const ui = SpreadsheetApp.getUi();
  
  const helpText = 
    '📚 YouTube Bulk Uploader Help\n\n' +
    'This menu allows you to manually trigger video uploads to YouTube.\n\n' +
    '🚀 Upload Videos Now\n' +
    '   Triggers the upload process immediately\n\n' +
    '📊 Check Function Status\n' +
    '   Verifies the Cloud Function is accessible\n\n' +
    '⚙️ Configure Function URL\n' +
    '   Set or update the Cloud Function URL\n\n' +
    'Configuration:\n' +
    '• Spreadsheet ID: ' + SPREADSHEET.getId() + '\n' +
    '• Drive Folder ID: ' + DRIVE_FOLDER_ID + '\n' +
    '• YouTube Channel ID: ' + YOUTUBE_CHANNEL_ID + '\n\n' +
    'For more information, visit the project README.';
  
  ui.alert('Help', helpText, ui.ButtonSet.OK);
}

/**
 * Refreshes the Logs sheet to show latest data
 */
function refreshLogsSheet() {
  try {
    const logsSheet = SPREADSHEET.getSheetByName('Logs');
    if (logsSheet) {
      // Just trigger a recalculation by touching a cell
      SpreadsheetApp.flush();
    }
  } catch (error) {
    Logger.log('Could not refresh logs sheet: ' + error);
  }
}

/**
 * Gets the Cloud Function URL from script properties or constant
 */
function getFunctionUrl() {
  const storedUrl = PropertiesService.getScriptProperties().getProperty('FUNCTION_URL');
  return storedUrl || CLOUD_FUNCTION_URL;
}

// ============================================================================
// ADVANCED: Time-based Trigger Setup
// ============================================================================

/**
 * Creates a time-based trigger to run uploads automatically
 * Run this function once to set up automatic uploads
 */
function createDailyTrigger() {
  // Delete existing triggers first
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'triggerUpload') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  // Create new daily trigger at 1 AM
  ScriptApp.newTrigger('triggerUpload')
    .timeBased()
    .atHour(1)
    .everyDays(1)
    .create();
  
  SpreadsheetApp.getUi().alert(
    '✅ Trigger Created',
    'A daily trigger has been set up to run uploads at 1 AM.',
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

/**
 * Removes all time-based triggers
 */
function removeAllTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    ScriptApp.deleteTrigger(trigger);
  });
  
  SpreadsheetApp.getUi().alert(
    '✅ Triggers Removed',
    'All automatic triggers have been removed.',
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}
