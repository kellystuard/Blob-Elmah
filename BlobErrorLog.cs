using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elmah;
using System.Collections;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.Web.Configuration;
using System.Xml;
using System.IO;
using System.Globalization;

namespace Elmah.Azure
{
    public class BlobErrorLog : ErrorLog
    {
        private string connectionStringName = null;

        public BlobErrorLog(IDictionary config)
        {
            if (!config.Contains("connectionStringName"))
                throw new System.ApplicationException("Configuration string is missing for the Windows Azure Blob Storage Error Log.");

            this.connectionStringName = (string)config["connectionStringName"];
            this.Initialize();
        }

        public BlobErrorLog(string connectionStringName)
        {
            this.connectionStringName = connectionStringName;
            this.Initialize();
        }

        private void Initialize()
        {
            var blobContainer = GetBlobContainer();
            blobContainer.CreateIfNotExist();
            blobContainer.SetPermissions(new BlobContainerPermissions() { PublicAccess = BlobContainerPublicAccessType.Blob });
        }

        private CloudBlobContainer GetBlobContainer()
        {
            var blobClient = GetStorageAccount().CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference("elmaherrors");
            return blobContainer;
        }

        private CloudStorageAccount GetStorageAccount()
        {
            var connectionString = WebConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
            return CloudStorageAccount.Parse(connectionString);
        }

        public override ErrorLogEntry GetError(string id)
        {
            ErrorLogEntry entry;
            try
            {
                id = new Guid(id).ToString();
            }
            catch (FormatException exception)
            {
                throw new ArgumentException(exception.Message, id, exception);
            }

            var blobContainer = GetBlobContainer();
            var blobs = blobContainer.ListBlobs();

            var blobItem = blobs.FirstOrDefault(b => b.Uri.ToString().EndsWith(id + ".xml"));
            if (blobItem == null)
                throw new FileNotFoundException(string.Format("Cannot locate error file for error with ID {0}.", id));

            using (var stream = new MemoryStream())
            {
                var blob = blobContainer.GetBlobReference(blobItem.Uri.ToString());
                blob.DownloadToStream(stream);
                stream.Position = 0;
                using (var reader = new XmlTextReader(stream))
                {
                    Error error = ErrorXml.Decode(reader);
                    entry = new ErrorLogEntry(this, id, error);
                }
            }
            return entry;
        }

        public override int GetErrors(int pageIndex, int pageSize, IList errorEntryList)
        {
            if (pageIndex < 0)
                throw new ArgumentOutOfRangeException("pageIndex", pageIndex, null);

            if (pageSize < 0)
                throw new ArgumentOutOfRangeException("pageSize", pageSize, null);

            var blobContainer = GetBlobContainer();
            var blobs = blobContainer.ListBlobs();
            if (blobs.Count() < 1)
                return 0;

            int count = 0;
            string[] keys = new string[blobs.Count()];

            foreach (var blob in blobs)
            {
                keys[count++] = blob.Uri.ToString();
            }

            Array.Sort(keys, 0, count, Comparer.DefaultInvariant);
            Array.Reverse(keys, 0, count);
            
            if (errorEntryList != null)
            {
                int firstIndex = pageIndex * pageSize;
                int lastIndex = (firstIndex + pageSize < count) ? firstIndex + pageSize : count;
                for (int i = firstIndex; i < lastIndex; i++)
                {
                    var blob = blobContainer.GetBlobReference(keys[i]);
                    using (var stream = new MemoryStream())
                    {
                        blob.DownloadToStream(stream);
                        stream.Position = 0;
                        using (XmlTextReader reader = new XmlTextReader(stream))
                        {
                            while (reader.IsStartElement("error"))
                            {
                                string attribute = reader.GetAttribute("errorId");
                                Error error = ErrorXml.Decode(reader);
                                errorEntryList.Add(new ErrorLogEntry(this, attribute, error));
                            }
                        }
                    }
                }
            }
            return count;
        }

        public override string Log(Error error)
        {
            string errorId = Guid.NewGuid().ToString();
            DateTime time = (error.Time > DateTime.MinValue) ? error.Time : DateTime.Now;
            string fileName = string.Format(CultureInfo.InvariantCulture, "error-{0:yyyy-MM-ddHHmmssZ}-{1}.xml", new object[] { time.ToUniversalTime(), errorId });

            var blobContainer = GetBlobContainer();
            var blob = blobContainer.GetBlobReference(fileName);

            using (var memory = new MemoryStream())
            {
                using (XmlTextWriter writer = new XmlTextWriter(memory, Encoding.UTF8))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.WriteStartElement("error");
                    writer.WriteAttributeString("errorId", errorId);
                    ErrorXml.Encode(error, writer);
                    writer.WriteEndElement();
                    writer.Flush();

                    memory.Position = 0;
                    blob.UploadFromStream(memory);
                }
            }
            return errorId;
        }

        public override string Name
        {
            get
            {
                return "Windows Azure Blob Storage Error Log";
            }
        }
    }
}
