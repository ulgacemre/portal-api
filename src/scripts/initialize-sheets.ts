/**
 * Google Sheets Initialization & Data Sync Script
 * 
 * This script can:
 * 1. Initialize the Google Sheet structure (create sheet, headers, formatting)
 * 2. Sync project data from the database to the sheet
 * 
 * Usage:
 * - Setup sheet structure only: npm run initialize-sheets -- --setup-only
 * - Sync all projects (includes setup): npm run initialize-sheets
 * - Sync all projects (data only): npm run initialize-sheets -- --sync-only
 * - Sync specific project: npm run initialize-sheets -- --project=<project-id>
 * - Full initialization + data sync: npm run initialize-sheets -- --full
 * 
 * Environment variables:
 * - INITIALIZE_SHEET: Set to 'true' to run the script (safety measure)
 */

import * as dotenv from 'dotenv';
import { initializeSheetWithAllProjects, syncProjectToSheets } from '../services/sheets-sync.service';
import { getGoogleAccessToken } from '../services/sheets-sync.service';
import axios from 'axios';

// Load environment variables
dotenv.config();

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const SHEET_NAME = 'Projects';

// Expected columns structure
const EXPECTED_COLUMNS = [
  "id", "level", "projectName","Test", "projectDescription", "projectVision", 
  "projectLinks", "referralSource", "scientificReferences", "credentialLinks", 
  "teamMembers", "motivation", "progress", 
  "id (Discord)", "serverId", "serverName", "invitationUrl",
  "messagesCount", "papersShared", "qualityScore", "memberCount", "lastActivity"
];

// Parse command line arguments
function parseArguments(): {
  mode: 'setup-only' | 'sync-only' | 'full' | 'specific-project';
  projectId?: string;
  help?: boolean;
} {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    return { mode: 'full', help: true };
  }
  
  if (args.includes('--setup-only')) {
    return { mode: 'setup-only' };
  }
  
  if (args.includes('--sync-only')) {
    return { mode: 'sync-only' };
  }
  
  const projectArg = args.find(arg => arg.startsWith('--project='));
  if (projectArg) {
    const projectId = projectArg.split('=')[1];
    if (!projectId) {
      throw new Error('Project ID is required when using --project flag');
    }
    return { mode: 'specific-project', projectId };
  }
  
  // Default mode is full initialization + sync
  return { mode: 'full' };
}

// Display help information
function showHelp(): void {
  console.log(`
Google Sheets Initialization & Data Sync Script

Usage:
  npm run initialize-sheets [options]

Options:
  --setup-only          Initialize sheet structure only (headers, formatting)
  --sync-only          Sync data only (assumes sheet structure exists)  
  --full               Full initialization + data sync (default)
  --project=<id>       Sync specific project by ID
  --help, -h           Show this help message

Environment Variables:
  INITIALIZE_SHEET     Must be set to 'true' to run the script

Examples:
  npm run initialize-sheets                           # Full setup + sync all projects
  npm run initialize-sheets -- --setup-only          # Setup sheet structure only
  npm run initialize-sheets -- --sync-only           # Sync all project data only
  npm run initialize-sheets -- --project=abc123      # Sync specific project
  npm run initialize-sheets -- --full                # Explicit full initialization
`);
}

/**
 * Sync project data to Google Sheets
 * - Syncs all projects or a specific project
 * - Uses the sheets-sync service functions
 */
async function syncProjectData(projectId?: string): Promise<void> {
  try {
    console.log('[SYNC] Starting project data synchronization');
    
    if (projectId) {
      console.log(`[SYNC] Syncing specific project: ${projectId}`);
      const result = await syncProjectToSheets(projectId);
      console.log(`[SYNC] Result: ${result}`);
    } else {
      console.log('[SYNC] Syncing all projects from database');
      const result = await initializeSheetWithAllProjects();
      console.log(`[SYNC] Result: ${result}`);
    }
    
    console.log('[SYNC] Project data synchronization complete!');
  } catch (error) {
    console.error('[SYNC] Data synchronization failed:', error);
    throw error;
  }
}

/**
 * Initialize Google Sheets structure for BioDAO project tracking
 * - Verifies spreadsheet access
 * - Creates Projects sheet if it doesn't exist
 * - Sets up column headers and formatting
 */
async function initializeSheetStructure(): Promise<void> {
  try {
    console.log('[INITIALIZE] Starting Google Sheets initialization');
    
    if (!SPREADSHEET_ID) {
      throw new Error('GOOGLE_SHEET_ID environment variable is not set');
    }
    
    const accessToken = await getGoogleAccessToken();
    console.log('[INITIALIZE] Successfully obtained Google API access token');
    
    // 1. First verify we can access the spreadsheet
    console.log(`[INITIALIZE] Verifying access to spreadsheet: ${SPREADSHEET_ID}`);
    try {
      const spreadsheetUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}?fields=properties`;
      const spreadsheetResponse = await axios.get(spreadsheetUrl, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });
      
      console.log(`[INITIALIZE] Successfully accessed spreadsheet: "${spreadsheetResponse.data.properties?.title}"`);
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        console.error(`[INITIALIZE] ERROR: Spreadsheet with ID ${SPREADSHEET_ID} not found.`);
        console.error('[INITIALIZE] Please check your GOOGLE_SHEET_ID environment variable.');
        console.error('[INITIALIZE] You may need to create a new spreadsheet and share it with your service account.');
        process.exit(1);
      } else {
        console.error('[INITIALIZE] Error accessing spreadsheet:', error);
        throw error;
      }
    }
    
    // 2. Check if the Projects sheet exists
    console.log(`[INITIALIZE] Checking if sheet "${SHEET_NAME}" exists`);
    let sheetExists = false;
    try {
      const sheetCheckUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:A1`;
      await axios.get(sheetCheckUrl, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });
      sheetExists = true;
      console.log(`[INITIALIZE] Sheet "${SHEET_NAME}" exists`);
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        console.log(`[INITIALIZE] Sheet "${SHEET_NAME}" does not exist - will create it`);
      } else {
        console.error('[INITIALIZE] Error checking if sheet exists:', error);
        throw error;
      }
    }
    
    // 3. Create the sheet if it doesn't exist
    if (!sheetExists) {
      console.log(`[INITIALIZE] Creating sheet "${SHEET_NAME}"`);
      try {
        const createSheetUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`;
        await axios.post(
          createSheetUrl, 
          {
            requests: [
              {
                addSheet: {
                  properties: {
                    title: SHEET_NAME
                  }
                }
              }
            ]
          },
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        console.log(`[INITIALIZE] Successfully created sheet "${SHEET_NAME}"`);
      } catch (error) {
        console.error(`[INITIALIZE] Error creating sheet "${SHEET_NAME}":`, error);
        throw error;
      }
    }
    
    // 4. Initialize headers
    console.log('[INITIALIZE] Setting up column headers');
    try {
      const headersUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:U1?valueInputOption=RAW`;
      await axios.put(
        headersUrl,
        { values: [EXPECTED_COLUMNS] },
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      console.log('[INITIALIZE] Successfully set up column headers');
    } catch (error) {
      console.error('[INITIALIZE] Error setting up headers:', error);
      throw error;
    }
    
    // 5. Format headers (make bold, freeze)
    console.log('[INITIALIZE] Formatting headers');
    try {
      const formatHeadersUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`;
      await axios.post(
        formatHeadersUrl,
        {
          requests: [
            // Make first row bold
            {
              repeatCell: {
                range: {
                  sheetId: 0,
                  startRowIndex: 0,
                  endRowIndex: 1
                },
                cell: {
                  userEnteredFormat: {
                    textFormat: {
                      bold: true
                    },
                    backgroundColor: {
                      red: 0.9,
                      green: 0.9,
                      blue: 0.9
                    }
                  }
                },
                fields: "userEnteredFormat(textFormat,backgroundColor)"
              }
            },
            // Freeze the header row
            {
              updateSheetProperties: {
                properties: {
                  sheetId: 0,
                  gridProperties: {
                    frozenRowCount: 1
                  }
                },
                fields: "gridProperties.frozenRowCount"
              }
            }
          ]
        },
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      console.log('[INITIALIZE] Successfully formatted headers');
    } catch (error) {
      console.error('[INITIALIZE] Error formatting headers:', error);
      // Non-critical error, continue
    }
    
    console.log('[INITIALIZE] Google Sheets initialization complete!');
    console.log('[INITIALIZE] Your spreadsheet is now ready for BioDAO project tracking.');
    console.log('[INITIALIZE] You can now run the sync scripts to populate data.');
  } catch (error) {
    console.error('[INITIALIZE] Initialization failed:', error);
    process.exit(1);
  }
}

/**
 * Main function to handle different execution modes
 */
async function main(): Promise<void> {
  try {
    // Check for safety environment variable
    if (process.env.INITIALIZE_SHEET !== 'true') {
      console.error('ERROR: INITIALIZE_SHEET environment variable must be set to "true" to run this script');
      console.error('This is a safety measure to prevent accidental execution');
      console.error('Usage: INITIALIZE_SHEET=true npm run initialize-sheets');
      process.exit(1);
    }

    const { mode, projectId, help } = parseArguments();
    
    if (help) {
      showHelp();
      return;
    }

    console.log(`[MAIN] Running in mode: ${mode}`);
    
    switch (mode) {
      case 'setup-only':
        await initializeSheetStructure();
        break;
        
      case 'sync-only':
        await syncProjectData(projectId);
        break;
        
      case 'specific-project':
        if (!projectId) {
          throw new Error('Project ID is required for specific project sync');
        }
        console.log(`[MAIN] Syncing specific project: ${projectId}`);
        await syncProjectData(projectId);
        break;
        
      case 'full':
      default:
        console.log('[MAIN] Running full initialization and data sync');
        await initializeSheetStructure();
        await syncProjectData();
        break;
    }
    
    console.log('[MAIN] Script completed successfully!');
  } catch (error) {
    console.error('[MAIN] Script failed:', error);
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  main().catch(console.error);
}

export { initializeSheetStructure, syncProjectData, main }; 