import paramiko, sys, os, io, json
from datetime import datetime

class transport_connection:
    def __init__(self, host, user_name=None, pw=None, port=22, priv_key=None,host_key=None):

        self.host = host
        self.user_name = user_name 
        self.pw = pw
        self.port = port 
        self.priv_key = priv_key
        self.host_key = host_key

    @staticmethod
    def manifest_file_create(source_file, target_file, source_file_size):
                            
        timestamp = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S") 
        manifest_filename = '{}.manifest'.format(target_file, timestamp)

        source_file_metadata = {
                'file_name': os.path.basename(source_file),
                'time_sent': timestamp,
                'file_byte_size': source_file_size,
            }
        return manifest_filename,source_file_metadata

    def send_file(self, source_file, target_file, meta_data=True, done_status=True, priv_key=None):
        """
        TODO: add the transformation between dbfs to local before the sftp.put() 
        """

        transport = paramiko.Transport(self.host)

        if self.host_key is not None:
            host_key = paramiko.RSAKey.from_private_key(self.host_key)

        else:
            host_key = self.host_key 
        
        # setup logging
        paramiko.util.log_to_file("/Users/glstream/Documents/GitHub/playground2/2020/sshTesting/ssh_module/logs/demo_sftp.log")

        passwd = self.pw if self.pw is not None else None
        user_name = self.user_name if self.user_name is not None else ""

        #PRIVATE KEY TERNERAY AND CREATING FILE OBJECT
        key_file_obj = io.StringIO(self.priv_key)
        priv_key_obj = paramiko.RSAKey.from_private_key(key_file_obj) if self.priv_key != None else None
        
        try:
            transport.connect(hostkey = host_key,
                            username = user_name,
                            password = passwd,
                            pkey = priv_key_obj
                            )

            print('Connection Successful.')    

        except (Exception) as e:
            return print('Error connecting to sftp: {}'.format(e))
            sys.exit(0)
        
        source_file_size = os.stat(source_file).st_size
        
        sftp_client = paramiko.SFTPClient.from_transport(transport,0,0) 
        try:
            sftp_client.put(localpath=source_file,
                            remotepath=target_file)

            print('Payload Sent.')
        except PermissionError as e:
            print("Error: {}".format(e))
        

        if meta_data is not False:
            meta_data = self.manifest_file_create(source_file, target_file, source_file_size)
            try:
                manifest_file = sftp_client.file(meta_data[0], 'a', -1)
                manifest_file.write(json.dumps(meta_data[1]))
                manifest_file.flush()
                print('Manifest file sent.')
            except Exception as e:
                print('Error while sending manifest file: {}'.format(e))

        if done_status is not False:
            done_filename = '{}_DONE'.format(target_file)
            try:
                done_file = sftp_client.file(done_filename, 'a', -1)
                done_file.flush()
                print('Done File Sent.')
            except Exception as e:
                print('Error while sending done file: {}'.format(e))
        sftp_client.close()
        print('Connection closed.')