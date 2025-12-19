#!/usr/bin/env python3
"""
Standalone Script for Reliance Automation Workflows
Runs Gmail attachment downloader and LlamaParse PDF processor with console logging
Scheduled to run every 3 hours using GitHub Actions
"""

import os
import json
import base64
import tempfile
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from io import BytesIO

# Remove schedule import since GitHub Actions will handle scheduling
# import schedule

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

# Define custom SUCCESS log level
logging.SUCCESS = 25  # Between INFO (20) and WARNING (30)
logging.addLevelName(logging.SUCCESS, "SUCCESS")

def success(self, message, *args, **kwargs):
    if self.isEnabledFor(logging.SUCCESS):
        self._log(logging.SUCCESS, message, args, **kwargs)

logging.Logger.success = success

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Configuration - now with environment variables
CONFIG = {
    'gmail': {
        'sender': os.environ.get('GMAIL_SENDER', "DONOTREPLY@ril.com"),
        'search_term': os.environ.get('GMAIL_SEARCH_TERM', "grn"),
        'days_back': int(os.environ.get('GMAIL_DAYS_BACK', 20)),
        'max_results': int(os.environ.get('GMAIL_MAX_RESULTS', 1000)),
        'gdrive_folder_id': os.environ.get('GDRIVE_FOLDER_ID', "1YH8bT01X0C03SbgFF8qWO49Tv85Xd5UU")
    },
    'pdf': {
        'drive_folder_id': os.environ.get('PDF_DRIVE_FOLDER_ID', "1BF7HA6JkAVVPjPhrR4yVq0zabqW6yZ6j"),
        'llama_agent': os.environ.get('LLAMA_AGENT', "Reliance Agent"),
        'spreadsheet_id': os.environ.get('SPREADSHEET_ID', "1zlJaRur0K50ZLFQhxxmvfFVA3l4Whpe9XWgi1E-HFhg"),
        'sheet_range': os.environ.get('SHEET_RANGE', "reliancegrn"),
        'days_back': int(os.environ.get('PDF_DAYS_BACK', 7)),
        'max_files': int(os.environ.get('PDF_MAX_FILES', 1000))
    },
    'log': {
        'sheet_range': os.environ.get('LOG_SHEET_RANGE', "workflow_logs")
    }
}

# Token file for persisting credentials
TOKEN_FILE = 'token.json'

# Default parameters
DEFAULT_SKIP_EXISTING = True

# Retry settings
MAX_RETRIES = 3
RETRY_WAIT = 2

class RelianceAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        self.processed_state_file = "processed_state.json"
        self.processed_emails = set()
        self.processed_pdfs = set()
        
        # Load processed state
        self._load_processed_state()
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive.file']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Initialize logs and stats
        self.logs = []
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }
    
    def _load_processed_state(self):
        """Load previously processed email and PDF IDs from file"""
        try:
            if os.path.exists(self.processed_state_file):
                with open(self.processed_state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_emails = set(state.get('emails', []))
                    self.processed_pdfs = set(state.get('pdfs', []))
        except Exception as e:
            logger.warning(f"Could not load processed state: {e}")
    
    def _save_processed_state(self):
        """Save processed email and PDF IDs to file"""
        try:
            state = {
                'emails': list(self.processed_emails),
                'pdfs': list(self.processed_pdfs)
            }
            with open(self.processed_state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Could not save processed state: {e}")
    
    def log(self, message: str, level: str = "INFO"):
        """Add log entry with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp, 
            "level": level.upper(), 
            "message": message
        }
        
        self.logs.append(log_entry)
        
        # Keep only last 100 logs to prevent memory issues
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]
        
        # Log to console
        level_map = {
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "SUCCESS": logging.SUCCESS
        }
        logger.log(level_map.get(level.upper(), logging.INFO), message)
    
    def get_logs(self):
        """Get logs"""
        return self.logs
    
    def clear_logs(self):
        """Clear all logs"""
        self.logs = []
    
    def reset_stats(self):
        """Reset stats before new run"""
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }
    
    def get_stats(self):
        """Get current stats"""
        return self.current_stats
    
    def authenticate(self):
        """Authenticate using token file for GitHub Actions"""
        try:
            self.log("Starting authentication process...", "INFO")
            
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            # For GitHub Actions, we'll create token.json from secret
            if os.path.exists(TOKEN_FILE):
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, combined_scopes)
                if creds and creds.valid:
                    self.log("Authentication successful using token.json!", "SUCCESS")
                elif creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    with open(TOKEN_FILE, 'w') as token:
                        token.write(creds.to_json())
                    self.log("Authentication successful after token refresh!", "SUCCESS")
                else:
                    self.log("Invalid token.json, authentication failed", "ERROR")
                    return False
            else:
                self.log(f"{TOKEN_FILE} not found", "ERROR")
                return False
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            return True
                
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False
    
    def retry_wrapper(self, func, *args, **kwargs):
        """Wrapper for retrying functions"""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.log(f"Attempt {attempt} failed: {str(e)}", "WARNING")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT)
                else:
                    raise
    
    def search_emails(self, sender: str = "", search_term: str = "",
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        def _search():
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            # Add date filter
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Searching Gmail with query: {query}", "INFO")
            
            # Execute search
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"Gmail search returned {len(messages)} messages", "INFO")
            
            return messages
        
        try:
            return self.retry_wrapper(_search)
        except Exception as e:
            self.log(f"Email search failed: {str(e)}", "ERROR")
            return []
    
    def get_existing_drive_ids(self, spreadsheet_id: str, sheet_range: str) -> set:
        """Get set of existing drive_file_id from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0]
            if "drive_file_id" not in headers:
                self.log("No 'drive_file_id' column found in sheet", "WARNING")
                return set()
            
            id_index = headers.index("drive_file_id")
            existing_ids = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            
            self.log(f"Found {len(existing_ids)} existing file IDs in sheet", "INFO")
            return existing_ids
            
        except Exception as e:
            self.log(f"Failed to get existing file IDs: {str(e)}", "ERROR")
            return set()
    
    def process_gmail_workflow(self, config: dict, progress_callback=None, status_callback=None):
        """Process Gmail attachment download workflow"""
        try:
            if status_callback:
                status_callback("Starting Gmail workflow...")
            self.log("Starting Gmail workflow", "INFO")
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            self.current_stats['gmail']['processed_emails'] = len(emails)
            
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {'success': True, 'processed': 0}
            
            if status_callback:
                status_callback(f"Found {len(emails)} emails. Processing attachments...")
            self.log(f"Found {len(emails)} emails matching criteria", "INFO")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                return {'success': False, 'processed': 0}
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                if email['id'] in self.processed_emails:
                    self.log(f"Skipping already processed email ID: {email['id']}", "INFO")
                    continue
                
                try:
                    if status_callback:
                        status_callback(f"Processing email {i+1}/{len(emails)}")
                    
                    # Get email details
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self.log(f"Processing email: {subject} from {sender}", "INFO")
                    
                    # Get full message with payload
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        self.log(f"No payload found for email: {subject}", "WARNING")
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    self.current_stats['gmail']['total_attachments'] += attachment_count
                    self.current_stats['gmail']['successful_uploads'] += attachment_count
                    
                    if attachment_count > 0:
                        processed_count += 1
                        self.processed_emails.add(email['id'])
                        self._save_processed_state()
                        self.log(f"Found {attachment_count} attachments in: {subject}", "SUCCESS")
                    else:
                        self.log(f"No matching attachments in: {subject}", "INFO")
                    
                except Exception as e:
                    self.log(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
                    self.current_stats['gmail']['failed_uploads'] += 1
            
            if status_callback:
                status_callback(f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails")
            self.log(f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails", "SUCCESS")
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"Failed to get email details for {message_id}: {str(e)}", "ERROR")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            self.log(f"Failed to create folder {folder_name}: {str(e)}", "ERROR")
            return ""
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, config: dict, base_folder_id: str) -> int:
        """Extract attachments from email with proper folder structure"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, config, base_folder_id
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create nested folder structure: Gmail_Attachments -> search_term -> file_type
                search_term = config.get('search_term', 'all-attachments')
                search_folder_name = search_term if search_term else "all-attachments"
                file_type_folder = self._classify_extension(filename)
                
                # Create search term folder
                search_folder_id = self._create_drive_folder(search_folder_name, base_folder_id)
                
                # Create file type folder within search folder
                type_folder_id = self._create_drive_folder(file_type_folder, search_folder_id)
                
                # Clean filename but do not add prefix
                clean_filename = self._sanitize_filename(filename)
                final_filename = clean_filename
                
                # Check if file already exists
                if not self._file_exists_in_folder(final_filename, type_folder_id):
                    # Upload to Drive
                    file_metadata = {
                        'name': final_filename,
                        'parents': [type_folder_id]
                    }
                    
                    media = MediaIoBaseUpload(
                        BytesIO(file_data),
                        mimetype='application/octet-stream',
                        resumable=True
                    )
                    
                    self.drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()
                    
                    self.log(f"Uploaded: {final_filename}", "INFO")
                    processed_count = 1
                else:
                    self.log(f"File already exists, skipping: {final_filename}", "INFO")
                
            except Exception as e:
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
        
        return processed_count
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        import re
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _classify_extension(self, filename: str) -> str:
        """Categorize file by extension"""
        if not filename or '.' not in filename:
            return "Other"
            
        ext = filename.split(".")[-1].lower()
        
        type_map = {
            "pdf": "PDFs",
            "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }
        
        return type_map.get(ext, "Other")
    
    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        """Check if file already exists in folder"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            return len(files) > 0
        except:
            return False
    
    def process_pdf_workflow(self, config: dict, progress_callback=None, status_callback=None, skip_existing: bool = False):
        """Process PDF workflow with LlamaParse"""
        try:
            if not LLAMA_AVAILABLE:
                self.log("LlamaParse not available. Install with: pip install llama-cloud-services", "ERROR")
                return {'success': False, 'processed': 0}
            
            # Set Llama API key from environment variable
            llama_api_key = os.environ.get('LLAMA_CLOUD_API_KEY')
            if not llama_api_key:
                self.log("LLAMA_CLOUD_API_KEY environment variable not set", "ERROR")
                return {'success': False, 'processed': 0}
            
            if status_callback:
                status_callback("Starting PDF processing workflow...")
            self.log("Starting PDF processing workflow...", "INFO")
            
            # Setup LlamaParse
            os.environ["LLAMA_CLOUD_API_KEY"] = llama_api_key
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                self.log(f"Could not find agent '{config['llama_agent']}'. Check LlamaParse dashboard.", "ERROR")
                return {'success': False, 'processed': 0}
            
            # Get existing IDs if skipping
            existing_ids = set()
            if skip_existing:
                existing_ids = self.get_existing_drive_ids(config['spreadsheet_id'], config['sheet_range'])
                self.log(f"Skipping {len(existing_ids)} already processed files", "INFO")
            
            # List PDF files from Drive
            pdf_files = self._list_drive_files(config['drive_folder_id'], config['days_back'])
            
            self.current_stats['pdf']['total_pdfs'] = len(pdf_files)
            
            if skip_existing:
                pdf_files = [f for f in pdf_files if f['id'] not in existing_ids]
                self.current_stats['pdf']['skipped_pdfs'] = self.current_stats['pdf']['total_pdfs'] - len(pdf_files)
                self.log(f"After filtering, {len(pdf_files)} PDFs to process", "INFO")
            
            # Apply max_files limit
            max_files = config.get('max_files', len(pdf_files))
            pdf_files = pdf_files[:max_files]
            
            if not pdf_files:
                self.log("No PDF files found in the specified folder", "WARNING")
                return {'success': True, 'processed': 0}
            
            if status_callback:
                status_callback(f"Found {len(pdf_files)} PDF files. Processing...")
            self.log(f"Found {len(pdf_files)} PDF files. Processing...", "INFO")
            
            # Get sheet info
            sheet_name = config['sheet_range'].split('!')[0]
            
            processed_count = 0
            total_rows = 0
            for i, file in enumerate(pdf_files):
                if file['id'] in self.processed_pdfs:
                    self.log(f"Skipping already processed PDF: {file['name']}", "INFO")
                    continue
                
                try:
                    if status_callback:
                        status_callback(f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}")
                    self.log(f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}", "INFO")
                    
                    # Download PDF
                    pdf_data = self._download_from_drive(file['id'], file['name'])
                    if not pdf_data:
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        continue
                    
                    # Process with LlamaParse
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(pdf_data)
                        temp_path = temp_file.name
                    
                    result = agent.extract(temp_path)
                    extracted_data = result.data
                    os.unlink(temp_path)
                    
                    # Process extracted data
                    rows = self._process_extracted_data(extracted_data, file)
                    if rows:
                        # Save to Google Sheets
                        self._save_to_sheets(config['spreadsheet_id'], sheet_name, rows, file['id'], sheet_id=self._get_sheet_id(config['spreadsheet_id'], sheet_name))
                        processed_count += 1
                        self.current_stats['pdf']['processed_pdfs'] += 1
                        self.current_stats['pdf']['rows_added'] += len(rows)
                        total_rows += len(rows)
                        self.processed_pdfs.add(file['id'])
                        self._save_processed_state()
                    
                except Exception as e:
                    self.log(f"Failed to process PDF {file['name']}: {str(e)}", "ERROR")
                    self.current_stats['pdf']['failed_pdfs'] += 1
            
            if status_callback:
                status_callback(f"PDF workflow completed! Processed {processed_count} PDFs")
            self.log(f"PDF workflow completed! Processed {processed_count} PDFs", "SUCCESS")
            
            return {'success': True, 'processed': processed_count, 'rows_added': total_rows}
            
        except Exception as e:
            self.log(f"PDF workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}
    
    def _list_drive_files(self, folder_id: str, days_back: int) -> List[Dict]:
        """List PDF files in Drive folder"""
        try:
            start_datetime = datetime.utcnow() - timedelta(days=days_back - 1)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime >= '{start_str}'"
            
            all_files = []
            page_token = None
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
                
                files = results.get('files', [])
                all_files.extend(files)
                
                page_token = results.get('nextPageToken', None)
                if page_token is None:
                    break
            
            return all_files
        except Exception as e:
            self.log(f"Failed to list files: {str(e)}", "ERROR")
            return []
    
    def _download_from_drive(self, file_id: str, file_name: str) -> bytes:
        """Download file from Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            return request.execute()
        except Exception as e:
            self.log(f"Failed to download {file_name}: {str(e)}", "ERROR")
            return b""
    
    def _process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        """Process extracted data from LlamaParse based on Reliance JSON structure"""
        rows = []
        items = []
        
        # Handle the provided JSON structure
        if "items" in extracted_data:
            items = extracted_data["items"]
            for item in items:
                item["po_number"] = self._get_value(extracted_data, ["po_number", "purchase_order_number", "PO No"])
                item["vendor_invoice_number"] = self._get_value(extracted_data, ["vendor_invoice_number", "invoice_number", "inv_no", "Invoice No"])
                item["supplier"] = self._get_value(extracted_data, ["Supplier Name", "supplier", "vendor"])
                item["shipping_address"] = self._get_value(extracted_data, ["delivery_address", "shipping_address", "receiver_address"])
                item["grn_date"] = self._get_value(extracted_data, ["grn_date", "delivered_on"])
                item["grn_number"] = self._get_value(extracted_data, ["grn_number"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']
        else:
            self.log(f"Skipping (no 'items' key found): {file_info['name']}", "WARNING")
            return rows
        
        # Clean items and add to rows
        for item in items:
            cleaned_item = {k: v for k, v in item.items() if v not in ["", None]}
            rows.append(cleaned_item)
        
        return rows
    
    def _get_value(self, data, possible_keys, default=""):
        """Return the first found key value from dict."""
        for key in possible_keys:
            if key in data:
                return data[key]
        return default
    
    def _save_to_sheets(self, spreadsheet_id: str, sheet_name: str, rows: List[Dict], file_id: str, sheet_id: int):
        """Save data to Google Sheets with proper header management and row replacement"""
        try:
            if not rows:
                return
            
            # Get existing headers and data
            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name)
            
            # Get all unique headers from new data
            new_headers = list(set().union(*(row.keys() for row in rows)))
            
            # Combine headers (existing + new unique ones)
            if existing_headers:
                all_headers = existing_headers.copy()
                for header in new_headers:
                    if header not in all_headers:
                        all_headers.append(header)
                
                # Update headers if new ones were added
                if len(all_headers) > len(existing_headers):
                    self._update_headers(spreadsheet_id, sheet_name, all_headers)
            else:
                # No existing headers, create them
                all_headers = new_headers
                self._update_headers(spreadsheet_id, sheet_name, all_headers)
            
            # Prepare values
            values = [[row.get(h, "") for h in all_headers] for row in rows]
            
            # Replace rows for this specific file
            self._replace_rows_for_file(spreadsheet_id, sheet_name, file_id, all_headers, values, sheet_id)
            
        except Exception as e:
            self.log(f"Failed to save to sheets: {str(e)}", "ERROR")
    
    def _get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            self.log(f"No existing headers found: {str(e)}", "INFO")
            return []
    
    def _update_headers(self, spreadsheet_id: str, sheet_name: str, headers: List[str]) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + len(headers))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.log(f"Updated headers with {len(headers)} columns", "INFO")
            return True
        except Exception as e:
            self.log(f"Failed to update headers: {str(e)}", "ERROR")
            return False
    
    def _get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Get the numeric sheet ID for the given sheet name"""
        try:
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for sheet in metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            self.log(f"Sheet '{sheet_name}' not found", "WARNING")
            return 0
        except Exception as e:
            self.log(f"Failed to get sheet metadata: {str(e)}", "ERROR")
            return 0
    
    def _get_sheet_data(self, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
        """Get all data from the sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                majorDimension="ROWS"
            ).execute()
            return result.get('values', [])
        except Exception as e:
            self.log(f"Failed to get sheet data: {str(e)}", "ERROR")
            return []
    
    def _replace_rows_for_file(self, spreadsheet_id: str, sheet_name: str, file_id: str,
                             headers: List[str], new_rows: List[List[Any]], sheet_id: int) -> bool:
        """Delete existing rows for the file if any, and append new rows"""
        try:
            values = self._get_sheet_data(spreadsheet_id, sheet_name)
            if not values:
                # No existing data, just append
                return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            
            current_headers = values[0]
            data_rows = values[1:]
            
            # Find file_id column
            try:
                file_id_col = current_headers.index('drive_file_id')
            except ValueError:
                self.log("No 'drive_file_id' column found, appending new rows", "INFO")
                return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            
            # Find rows to delete (matching file_id)
            rows_to_delete = []
            for idx, row in enumerate(data_rows, 2):  # Start from row 2 (after header)
                if len(row) > file_id_col and row[file_id_col] == file_id:
                    rows_to_delete.append(idx)
            
            # Delete existing rows for this file
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)  # Delete from bottom to top
                requests = []
                for row_idx in rows_to_delete:
                    requests.append({
                        'deleteDimension': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'ROWS',
                                'startIndex': row_idx - 1,  # 0-indexed
                                'endIndex': row_idx
                            }
                        }
                    })
                
                if requests:
                    body = {'requests': requests}
                    self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=body
                    ).execute()
                    self.log(f"Deleted {len(rows_to_delete)} existing rows for file {file_id}", "INFO")
            
            # Append new rows
            return self._append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            
        except Exception as e:
            self.log(f"Failed to replace rows: {str(e)}", "ERROR")
            return False
    
    def _append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                self.log(f"Appended {updated_cells} cells to Google Sheet", "INFO")
                return True
            except Exception as e:
                if attempt < max_retries:
                    self.log(f"Failed to append to Google Sheet (attempt {attempt}/{max_retries}): {str(e)}", "WARNING")
                    time.sleep(wait_time)
                else:
                    self.log(f"Failed to append to Google Sheet after {max_retries} attempts: {str(e)}", "ERROR")
                    return False
        return False

    def _log_workflow_to_sheet(self, spreadsheet_id: str, sheet_range: str, start_time: datetime, end_time: datetime, stats: Dict):
        """Log workflow information to Google Sheet"""
        try:
            sheet_name = sheet_range.split('!')[0] if '!' in sheet_range else sheet_range
            
            # Define headers
            headers = [
                "Start Time", "End Time",
                "Gmail Processed Emails", "Gmail Total Attachments", "Gmail Successful Uploads", "Gmail Failed Uploads",
                "PDF Total PDFs", "PDF Processed PDFs", "PDF Skipped PDFs", "PDF Failed PDFs", "PDF Rows Added"
            ]
            
            # Prepare log row
            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                stats['gmail']['processed_emails'],
                stats['gmail']['total_attachments'],
                stats['gmail']['successful_uploads'],
                stats['gmail']['failed_uploads'],
                stats['pdf']['total_pdfs'],
                stats['pdf']['processed_pdfs'],
                stats['pdf']['skipped_pdfs'],
                stats['pdf']['failed_pdfs'],
                stats['pdf']['rows_added']
            ]
            
            # Get existing data to check if headers exist
            values = self._get_sheet_data(spreadsheet_id, sheet_name)
            
            if not values:
                # No data, append headers first
                self._append_to_google_sheet(spreadsheet_id, sheet_range, [headers])
            
            elif values[0] != headers:
                # Headers don't match, update headers
                self._update_headers(spreadsheet_id, sheet_name, headers)
            
            # Append the log row
            success = self._append_to_google_sheet(spreadsheet_id, sheet_range, [log_row])
            if success:
                self.log("Successfully logged workflow information to sheet", "SUCCESS")
            else:
                self.log("Failed to log workflow information to sheet", "ERROR")
            
        except Exception as e:
            self.log(f"Failed to log to sheet: {str(e)}", "ERROR")

def run_combined_workflow(automation):
    """Run Gmail to Drive then PDF to Sheet workflows"""
    start_time = datetime.now()
    automation.log("Starting combined workflow...")
    automation.reset_stats()
    
    gmail_config = CONFIG['gmail']
    
    pdf_config = CONFIG['pdf']
    
    def update_progress(value):
        automation.log(f"Progress: {value}%")
    
    def update_status(message):
        automation.log(message)
    
    gmail_result = automation.process_gmail_workflow(
        gmail_config, 
        progress_callback=update_progress,
        status_callback=update_status
    )
    
    if not gmail_result['success']:
        automation.log("Gmail to Drive failed. Stopping combined workflow.", "ERROR")
        end_time = datetime.now()
        automation._log_workflow_to_sheet(pdf_config['spreadsheet_id'], CONFIG['log']['sheet_range'], start_time, end_time, automation.current_stats)
        return {"status": "failed", "message": "Gmail to Drive failed"}
    
    pdf_result = automation.process_pdf_workflow(
        pdf_config, 
        progress_callback=update_progress,
        status_callback=update_status,
        skip_existing=DEFAULT_SKIP_EXISTING
    )
    
    end_time = datetime.now()
    automation.log(f"Combined workflow completed! Gmail: Processed {gmail_result['processed']} attachments. PDF: Processed {pdf_result['processed']} PDFs (skipped {automation.current_stats['pdf']['skipped_pdfs']}), added {pdf_result.get('rows_added', 0)} rows.")
    
    # Log to sheet
    automation._log_workflow_to_sheet(pdf_config['spreadsheet_id'], CONFIG['log']['sheet_range'], start_time, end_time, automation.current_stats)
    
    return {
        "status": "success",
        "gmail_attachments": gmail_result['processed'],
        "pdf_pdfs": pdf_result['processed'],
        "pdf_skipped": automation.current_stats['pdf']['skipped_pdfs'],
        "pdf_rows": pdf_result.get('rows_added', 0)
    }

def main():
    """Main function to run the automation - single run for GitHub Actions"""
    automation = RelianceAutomation()
    
    if not automation.authenticate():
        logger.error("Authentication failed. Exiting.")
        return
    
    # Run once (GitHub Actions handles scheduling)
    result = run_combined_workflow(automation)
    logger.info(f"Workflow result: {result}")

if __name__ == "__main__":
    main()
