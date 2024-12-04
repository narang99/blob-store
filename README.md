blob-store is a library providing a simple interface to storing your files in a cloud agnostic fashion  

# Features
- Cloud-agnostic storage of files
- Serialisation + De-serialisation: Allowing you to move your path objects around different processes, making it easy to handle remote file locations
- Easy interactions between different kinds of cloud locations
  - You could run `S3BlobPath.cp(AzureBlobPath)` and it would just work

Currently performance is not a major concern, ease of use is. I would focus on performance once I'm satisfied by the API this library has produced

# Usage
Check out the usage [notebook](./docs/usage.ipynb)

# Roadmap

- [ ] Add other storage backends
  -  [ ] GCP, Minio
- [ ] Pre-signed URLs
- [ ] Make separate packages for each storage backend, a plugin architecture
  - This will be done so that the library does not look bloated
  - Or I could use `extra` params
- [ ] Figure out how we can provide ways to hook backend specific args
  - It would be best for me to provide the fastest default behavior, but we might want people to add custom args to their specific backend
- [ ] Tests
- [ ] File object that does not rely on filesystem
- [ ] Caching support
- [ ] Figure out a better interface to tweak how operations are done for specific cloud provider
  - how to get customised boto3 session?
  - how to do a customised upload, with different params
- [ ] Optimisation of auxiliary operations like copy
- [ ] More shortcuts

