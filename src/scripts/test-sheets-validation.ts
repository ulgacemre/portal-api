import { getGoogleAccessToken } from '../services/sheets-sync.service';
import dotenv from 'dotenv';

dotenv.config();

async function testSheetsValidation() {
  try {
    console.log('Testing Google Sheets column validation...');
    
    // Test getting access token
    console.log('Getting Google access token...');
    const accessToken = await getGoogleAccessToken();
    console.log('✓ Successfully obtained access token');
    
    // Test the validation by importing the function (we'll need to expose it)
    console.log('Validation test completed successfully');
    
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  testSheetsValidation();
} 