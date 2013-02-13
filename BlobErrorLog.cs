using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Elmah.Azure
{
    public sealed class BlobErrorLog : ErrorLog
    {
        public BlobErrorLog(IDictionary config)
        {
            if (config == null) throw new ArgumentNullException("config");
            if (!config.Contains("connectionStringName"))
                throw new System.ApplicationException("Configuration string is missing for the Windows Azure Blob Storage Error Log.");

            _connectionString = RoleEnvironment.GetConfigurationSettingValue((string)config["connectionStringName"]);
            Initialize();
        }

        public BlobErrorLog(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException("connectionString");

            this._connectionString = connectionString;
            this.Initialize();
        }

        public override ErrorLogEntry GetError(string id)
        {
            if (id == null) throw new ArgumentNullException("id");

            var blobContainer = GetBlobContainer();
            var blob = blobContainer.GetBlockBlobReference(id);

            using (var blobStream = blob.OpenRead())
            using (var reader = new XmlTextReader(blobStream))
            {
                var error = ErrorXml.Decode(reader);
                return new ErrorLogEntry(this, id, error);
            }
        }

        public override int GetErrors(int pageIndex, int pageSize, IList errorEntryList)
        {
            if (pageIndex < 0) throw new ArgumentOutOfRangeException("pageIndex", pageIndex, null);
            if (pageSize < 0) throw new ArgumentOutOfRangeException("pageSize", pageSize, null);
            if (errorEntryList == null) throw new ArgumentNullException("errorEntryList");

            var blobContainer = GetBlobContainer();
            var blobs = blobContainer.ListBlobs()
                .OfType<CloudBlockBlob>()
                .Skip(pageIndex * pageSize)
                .Take(pageSize);

            var count = 0;
            foreach (var blob in blobs)
            {
                count++;
                using (var blobStream = blob.OpenRead())
                using (var reader = new XmlTextReader(blobStream))
                {
                    var error = ErrorXml.Decode(reader);
                    errorEntryList.Add(new ErrorLogEntry(this, blob.Name, error));
                }
            }
            return count;
        }

        public override string Log(Error error)
        {
            var errorId = string.Format(CultureInfo.InvariantCulture, "error-{0:x16}-{1:N}.xml",
                (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks),
                Guid.NewGuid()
            );

            var blobContainer = GetBlobContainer();
            var blob = blobContainer.GetBlockBlobReference(errorId);

            using (var blobStream = blob.OpenWrite())
            using (var writer = new XmlTextWriter(blobStream, Encoding.UTF8))
            {
                writer.WriteStartElement("error");
                ErrorXml.Encode(error, writer);
                writer.WriteEndElement();
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

        private void Initialize()
        {
            var blobContainer = GetBlobContainer();
            blobContainer.CreateIfNotExists();
        }

        private CloudBlobContainer GetBlobContainer()
        {
            var blobClient = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference("elmaherrors");
            return blobContainer;
        }

        private string NewLogId()
        {
            return (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19")
                + '-' + Guid.NewGuid().ToString("n");
        }

        private readonly string _connectionString;
    }
}
