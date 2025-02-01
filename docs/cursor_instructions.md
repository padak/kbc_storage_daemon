# Init conversation

Prompt
```
this is @Keboola LLMS - I want to create "daemon" - script which is running on background, checking one specific folder on my computer. If I create sub-folder, this daemon will create bucket in Keboola Storage in IN stage (folder "test" will be bucket "test" in stage "in"). If I create new CSV file in the subfolder, my daemon will create table in Keboola Storage and then check if the CSV file is updated in of so, then it will upload it to Keboola table as a FULL LOAD.

Daemon will ignore all non-CSV tables. Daemon has to have a configuration when is stored API token and Keboola stack endpoint (URL of Keboola).

What I missed? Ask me a clarifying questions first and then create instructions into file @production_design.md for developers who will be working on this project.

It should be lightwave, easy to maintain and very well documented. We can implement everything on our own or use Keboola Python SDK.

Keboola Storage API has documentation here @https://keboola.docs.apiary.io/# 
Keboola Python SDK has documentation here @https://developers.keboola.com/integrate/storage/python-client/  and repo here @https://github.com/keboola/sapi-python-client/ 
```

## Clarifying Questions
1. Folder Monitoring
 - What mechanism do you prefer for folder monitoring? For example, should we use a polling approach (e.g., every few seconds) or an event-based watcher (such as Python’s watchdog library)?
 - How frequently should the daemon check for new sub-folders or changes inside them?
2. Bucket and Table Management
 - Should the daemon perform a check to verify if a bucket already exists for a given sub-folder before attempting to create a new one?
 - For CSV files: do we expect a new table to be created each time a new CSV is detected, or should we update an existing table if one exists for that CSV?
 - When an update is detected on a CSV file, do we always perform a FULL LOAD (i.e. delete and replace), or should we support an incremental load option in the future?
3. File Validity and Filtering
 - The requirement states that non-CSV files will be ignored. Should we also verify that CSV files have the expected format before uploading (for example, checking for headers, file size, or a specific naming convention)?
4. Configuration
 - What configuration format do you prefer (JSON, YAML, plain INI file)? This configuration should include at least the Storage API token and Keboola stack endpoint.
 - Should the configuration allow other parameters (like polling interval, error retry limits, log level, etc.)?
5. Error Handling and Logging
 - What should be the behavior in case of API failover (e.g., should the daemon retry with exponential backoff)?
 - Should detailed logging be implemented to track changes, errors, and successful uploads? If so, do you have any specifications regarding log file locations or verbosity?
6. Development Approach
 - Would you like to implement everything from scratch, or do you prefer leveraging the Keboola Python SDK (kbcstorage) for operations such as creating buckets, tables, and uploading CSVs?
 - Should the daemon be a standalone script, or do you envision packaging it as a module or service (e.g., using a systemd service or containerized solution)?

### Answers
```
1. event-based watcher is better
2. yes, check Storage consistency - if the bucket exists; create new table only during the first load, then use full load to existing table; do not use incremental import as of now
3. yes, check the header of update local CSV file and remote CSV header (you can store headers from the first load or check it whenever we do full import)
4. cfg file is up to you; yes, if we have any constant or variable in the daemon, make it configurable in the configuration
5. log errors to file and let the verbosity configurable through configuration file 
6. use Keboola SDK and make everything running in the docker 
```

## Further questions

1. Configuration Format: 
 - The design doc mentions JSON, YAML, or another format for configuration. Do you have a preference for the configuration file format?
2. Logging Strategy:
 - Do you want logs to be written to files, stdout, or both?
 - Should we implement log rotation?
 - Do you need any specific logging format (e.g., JSON structured logging)?
3. Error Handling:
 - What should happen if a CSV file's header changes? Just log an error and skip, or should we notify somewhere?
 - Should we implement a dead letter queue or similar mechanism for failed operations?
4. Monitoring & Health:
 - Do you need any health check endpoints or monitoring capabilities?
 - Should we expose any metrics (e.g., Prometheus)?
5. Testing Strategy:
 - Do you want integration tests with actual Keboola API?
 - Should we mock the filesystem events for testing?
6. Deployment & Operations:
 - What's the target environment for deployment?
 - Do you need any specific Docker base image?
 - Any specific resource constraints we should consider?
7. CSV Processing:
 - Should we support different CSV dialects (different delimiters, quote characters)?
 - Do we need to handle character encoding issues?
 - Is there a maximum file size we should consider?
8. Security:
 - How should we handle API tokens? Environment variables or config file?
 - Do we need any specific file permissions handling?

 ### Answers

 ```
1. no preferrence
2. it is daemon - log to file, rotate logs after 100MB, logformat is up to you
3. if header changed, just log it
4. no monitoring as of now, just save this idea (prometheus, etc.) to "TODO" in @activity_log.md 
5. no testing - store this idea to TODO
6. I'll be running it on my OSX, chose lightwave docker base image
7. detect CSV dialects and properly notify Keboola when configuring CSV upload, support only UTF8, filesize is unlimited (let's gzip upload when data are bigger than 50MB)
8. API tokens in .env
```
