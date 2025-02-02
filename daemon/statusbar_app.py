# daemon/statusbar_app.py
import sys
print("Starting KBC Daemon...")  # Debug print

import rumps
import os
import signal
import threading
import subprocess
from dotenv import load_dotenv, set_key
from pathlib import Path
from typing import Optional, List, Dict
from Foundation import NSOpenPanel
from AppKit import NSApp
from .storage_client import StorageClient
from .main import DaemonContext
from .config import Config

print("Imports completed")  # Debug print

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

print(f"Loaded environment from {env_path}")  # Debug print

class StatusBarApp(rumps.App):
    def __init__(self):
        print("Initializing StatusBarApp...")  # Debug print
        super().__init__(
            name="KBC Daemon",
            title="🔴 KBC",  # Shorter title for status bar
            icon=None  # Remove icon for now to simplify
        )
        self.storage_client: Optional[StorageClient] = None
        self.selected_bucket: Optional[Dict] = None
        self.watched_directory: Optional[Path] = None
        self.daemon_context: Optional[DaemonContext] = None
        self.daemon_thread: Optional[threading.Thread] = None
        
        # Create submenu for buckets
        self.buckets_menu = rumps.MenuItem("Select Bucket", callback=None)
        self.buckets_menu.update(["Loading buckets..."])
        
        # Initialize menu items (remove duplicate Quit)
        self.menu = [
            self.buckets_menu,
            "Select Watch Folder",
            None,  # Separator
            "Show Current Settings",
            "Edit API Settings",
            None,  # Separator
            "Start Monitoring",
            "Stop Monitoring",
            None  # Separator
            # Quit is automatically added by rumps
        ]
        
        print("Menu initialized")  # Debug print
        
        # Initialize storage client and load buckets
        self._init_storage_client()
        if self.storage_client:
            self._load_buckets()
            # Restore previous bucket selection
            self._restore_bucket_selection()
        
        # Restore previous watch folder
        self._restore_watch_folder()
        print("StatusBarApp initialization completed")  # Debug print
    
    def _handle_signal(self, signum, frame):
        """Handle termination signals."""
        signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
        print(f"\nReceived {signal_name}, shutting down...")
        
        # Stop the daemon if it's running
        if self.daemon_context:
            self.stop_monitoring(None)
        
        # Quit the app
        self.quit_app(None)

    def _init_storage_client(self):
        """Initialize the Keboola Storage client."""
        api_token = os.getenv("KEBOOLA_API_TOKEN")
        stack_url = os.getenv("KEBOOLA_STACK_URL")
        
        if api_token and stack_url:
            try:
                self.storage_client = StorageClient(
                    api_token=api_token,
                    stack_url=stack_url
                )
                self.title = "🔴 KBC Daemon"  # Red when not monitoring
            except Exception as e:
                self.title = "🔴 KBC Daemon"
                rumps.notification(
                    "Connection Error",
                    "",
                    f"Failed to connect to Keboola: {str(e)}"
                )
        else:
            self.title = "🔴 KBC Daemon"
            rumps.notification(
                "Configuration Missing",
                "",
                "Please set up your API credentials"
            )

    def _load_buckets(self):
        """Load buckets into the menu."""
        try:
            # Get list of buckets
            buckets = self.storage_client._buckets
            if not buckets:
                self.buckets_menu.update(["No buckets found"])
                return
            
            # Create menu items for buckets
            bucket_items = []
            for bucket in buckets:
                menu_item = rumps.MenuItem(
                    bucket['name'],
                    callback=self._on_bucket_selected
                )
                # Set checkmark if this bucket is selected
                if (self.selected_bucket and 
                    self.selected_bucket['id'] == bucket['id']):
                    menu_item.state = 1
                bucket_items.append(menu_item)
            
            # Update the buckets submenu
            self.buckets_menu.update(bucket_items)
            
        except Exception as e:
            self.buckets_menu.update([f"Error: {str(e)}"])

    def _restore_bucket_selection(self):
        """Restore previously selected bucket."""
        try:
            saved_bucket_id = os.getenv("SELECTED_BUCKET_ID")
            if saved_bucket_id and self.storage_client:
                buckets = self.storage_client._buckets
                self.selected_bucket = next(
                    (b for b in buckets if b['id'] == saved_bucket_id),
                    None
                )
                if self.selected_bucket:
                    # Update menu checkmark
                    for item in self.buckets_menu.values():
                        if item.title == self.selected_bucket['name']:
                            item.state = 1
                            break
        except Exception as e:
            print(f"Error restoring bucket selection: {e}")

    def _restore_watch_folder(self):
        """Restore previously selected watch folder."""
        try:
            saved_folder = os.getenv("WATCHED_DIRECTORY")
            if saved_folder:
                folder_path = Path(saved_folder)
                if folder_path.exists() and folder_path.is_dir():
                    self.watched_directory = folder_path
        except Exception as e:
            print(f"Error restoring watch folder: {e}")

    def _on_bucket_selected(self, sender):
        """Handle bucket selection from menu."""
        try:
            # Uncheck all buckets
            for item in self.buckets_menu.values():
                item.state = 0
            
            # Check the selected bucket
            sender.state = 1
            
            # Find the selected bucket data
            buckets = self.storage_client._buckets
            self.selected_bucket = next(
                (b for b in buckets if b['name'] == sender.title),
                None
            )
            
            if self.selected_bucket:
                # Save selection to .env
                set_key(str(env_path), "SELECTED_BUCKET_ID", self.selected_bucket['id'])
                
                rumps.notification(
                    "Bucket Selected",
                    "",
                    f"Selected bucket: {sender.title}"
                )
            
        except Exception as e:
            rumps.alert("Error", f"Failed to select bucket: {str(e)}")
            sender.state = 0

    def _run_daemon(self):
        """Run the daemon in a separate thread."""
        try:
            # Create a new Config instance with our settings
            config = {
                'keboola_api_token': os.getenv("KEBOOLA_API_TOKEN", ""),
                'keboola_stack_url': os.getenv("KEBOOLA_STACK_URL", ""),
                'watched_directory': str(self.watched_directory),
                'selected_bucket_id': self.selected_bucket["id"],
                'log_level': "INFO",
                'log_file': "daemon.log",
                'log_dir': "./logs"
            }
            
            # Update environment variables
            for key, value in config.items():
                os.environ[key.upper()] = str(value)
            
            # Initialize daemon without signal handling
            self.daemon_context = DaemonContext(handle_signals=False)
            
            # Start monitoring
            with self.daemon_context as daemon:
                daemon.wait_for_shutdown()
                
        except Exception as e:
            rumps.notification(
                "Daemon Error",
                "",
                f"Error running daemon: {str(e)}"
            )
            self.title = "🔴 KBC Daemon"  # Red on error

    @rumps.clicked("Select Watch Folder")
    def select_watch_folder(self, _):
        """Show folder selection dialog using native macOS picker."""
        if not self.selected_bucket:
            rumps.alert("Error", "Please select a bucket first")
            return
        
        try:
            # AppleScript to show folder picker
            script = '''
            tell application "System Events"
                activate
                set folderPath to POSIX path of (choose folder with prompt "Select folder to monitor for CSV files")
            end tell
            '''
            
            # Run AppleScript and get the selected folder path
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                folder_path = result.stdout.strip()
                if folder_path:
                    self.watched_directory = Path(folder_path)
                    # Update environment variable
                    set_key(str(env_path), "WATCHED_DIRECTORY", str(folder_path))
                    rumps.notification(
                        "Folder Selected",
                        "",
                        f"Monitoring folder: {folder_path}"
                    )
        except Exception as e:
            rumps.alert("Error", f"Failed to select folder: {str(e)}")

    @rumps.clicked("Show Current Settings")
    def show_settings(self, _):
        """Show current configuration."""
        try:
            # Prepare settings with safe access to avoid potential None access
            bucket_name = "None"
            if self.selected_bucket and isinstance(self.selected_bucket, dict):
                bucket_name = self.selected_bucket.get('name', 'None')
            
            watched_dir = str(self.watched_directory) if self.watched_directory else "None"
            
            # Format settings with proper spacing
            settings = (
                f"🔑  API Token\n"
                f"    {'Configured' if os.getenv('KEBOOLA_API_TOKEN') else 'Not configured'}\n"
                f"\n"
                f"🌐  Stack URL\n"
                f"    {os.getenv('KEBOOLA_STACK_URL', 'Not configured')}\n"
                f"\n"
                f"📦  Selected Bucket\n"
                f"    {bucket_name}\n"
                f"\n"
                f"📁  Watched Directory\n"
                f"    {watched_dir}"
            )
            
            # Create window without text input
            window = rumps.Window(
                title="Current Settings",
                message=settings,
                dimensions=(400, 200)
            )
            window.default_text = ""  # Empty default text
            window.add_button("Close")  # Only add close button
            
            # Bring Python app to front
            subprocess.run([
                'osascript',
                '-e',
                'tell application "System Events" to set frontmost of process "Python" to true'
            ])
            
            window.run()
            
        except Exception as e:
            print(f"Error showing settings: {str(e)}")  # Console logging
            try:
                rumps.alert(
                    title="Error",
                    message=f"Failed to show settings: {str(e)}"
                )
            except:
                pass

    @rumps.clicked("Edit API Settings")
    def edit_settings(self, _):
        """Edit API credentials."""
        # API Token window
        token_window = rumps.Window(
            title="Update API Token",
            message="Enter Keboola API Token:",
            default_text=os.getenv("KEBOOLA_API_TOKEN", ""),
            dimensions=(300, 100)
        )
        token_response = token_window.run()
        
        # Stack URL window
        url_window = rumps.Window(
            title="Update Stack URL",
            message="Enter Keboola Stack URL:",
            default_text=os.getenv("KEBOOLA_STACK_URL", ""),
            dimensions=(300, 100)
        )
        url_response = url_window.run()
        
        if token_response.clicked == 1 and url_response.clicked == 1:
            # Save new values
            set_key(str(env_path), "KEBOOLA_API_TOKEN", token_response.text)
            set_key(str(env_path), "KEBOOLA_STACK_URL", url_response.text)
            
            # Reinitialize storage client and reload buckets
            self._init_storage_client()
            if self.storage_client:
                self._load_buckets()
            
            rumps.notification(
                "Settings Updated",
                "",
                "API settings have been updated"
            )

    @rumps.clicked("Start Monitoring")
    def start_monitoring(self, _):
        """Start the file monitoring daemon."""
        if not self.selected_bucket or not self.watched_directory:
            rumps.alert(
                "Error",
                "Please select both a bucket and a watch folder first"
            )
            return
        
        if self.daemon_context:
            rumps.alert("Error", "Daemon is already running")
            return
        
        try:
            # Create logs directory if it doesn't exist
            logs_dir = Path("./logs")
            logs_dir.mkdir(exist_ok=True)
            
            # Start daemon in a separate thread
            self.daemon_thread = threading.Thread(target=self._run_daemon)
            self.daemon_thread.start()
            
            self.title = "🟢 KBC Daemon"  # Green when monitoring
            rumps.notification(
                "Monitoring Started",
                "",
                f"Watching {self.watched_directory} for CSV files"
            )
        except Exception as e:
            self.daemon_context = None
            self.daemon_thread = None
            self.title = "🔴 KBC Daemon"  # Red on error
            rumps.alert("Error", f"Failed to start daemon: {str(e)}")

    @rumps.clicked("Stop Monitoring")
    def stop_monitoring(self, _):
        """Stop the file monitoring daemon."""
        if not self.daemon_context:
            rumps.alert("Error", "Daemon is not running")
            return
        
        try:
            # Signal the daemon to stop
            if self.daemon_context:
                self.daemon_context._shutdown_event.set()
                if self.daemon_thread:
                    self.daemon_thread.join(timeout=5.0)
            
            self.daemon_context = None
            self.daemon_thread = None
            self.title = "🔴 KBC Daemon"  # Red when not monitoring
            
            rumps.notification(
                "Monitoring Stopped",
                "",
                "File monitoring has been stopped"
            )
        except Exception as e:
            rumps.alert("Error", f"Failed to stop daemon: {str(e)}")

    @rumps.clicked("Quit")
    def quit_app(self, _):
        """Quit the application."""
        print("Shutting down KBC Daemon...")
        # Stop the daemon if it's running
        if self.daemon_context:
            try:
                self.stop_monitoring(None)
            except Exception as e:
                print(f"Error stopping daemon: {e}")
        
        # Actually quit the app
        print("Goodbye!")
        rumps.quit_application()

if __name__ == "__main__":
    try:
        print("Creating StatusBarApp instance...")  # Debug print
        app = StatusBarApp()
        print("Running StatusBarApp...")  # Debug print
        app.run()
    except Exception as e:
        print(f"Error starting app: {e}", file=sys.stderr)
        sys.exit(1)