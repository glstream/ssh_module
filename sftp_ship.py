import paramiko, sys, os, io, json, tempfile
from datetime import datetime

class ssh_connection:
    def __init__(self, 
                host, 
                userName, 
                pw=None, 
                port=22, 
                pub_key=None, 
                priv_key=None):

        self.host = host
        self.userName = userName 
        self.pw = pw
        self.port = port 
        self.pub_key = pub_key
        self.priv_key = priv_key


    @staticmethod
    def pub_key_temp_file_name(pub_key):
        pub_key_bytes_obj = pub_key.encode()
        temp = tempfile.NamedTemporaryFile(delete=False)
        temp.write(pub_key_bytes_obj)
        file_name = temp.name
        return file_name

    @staticmethod
    def manifest_file_create(source_file, 
                            target_file, 
                            source_file_size):
                            
        timestamp = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S") 
        manifest_filename = '{}.manifest'.format(target_file, timestamp)

        source_file_metadata = {
                'file_name': os.path.basename(source_file),
                'time_sent': timestamp,
                'file_byte_size': source_file_size,
            }
        return manifest_filename,source_file_metadata

    def send_file(self, source_file, target_file, meta_data=True, done_status=True, pub_key=None, priv_key=None):
        """
        TODO: add the transformation between dbfs to local before the sftp.put() 
        """
        ssh_client=paramiko.SSHClient()
        # ssh_client.load_host_keys('/Users/glstream/Documents/GitHub/playground2/2020/sshTesting/ssh_key/pub_key')
        if self.pub_key is not None:
            # pub_key_temp_file_name =  self.pub_key_temp_file_name()
            pub_key_temp_file_name =  self.pub_key_temp_file_name(self.pub_key)
            ssh_client.load_host_keys(pub_key_temp_file_name)

        else:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # setup logging
        paramiko.util.log_to_file("/Users/glstream/Documents/GitHub/playground2/2020/sshTesting/ssh_module/logs/demo_sftp.log")

        passwd = self.pw if self.pw is not None else None

        #PRIVATE KEY TERNERAY AND CREATING FILE OBJECT
        key_file_obj = io.StringIO(self.priv_key)
        priv_key_obj = paramiko.RSAKey.from_private_key(key_file_obj) if self.priv_key != None else None
        
        try:
            ssh_client.connect(hostname= self.host
                                ,username=self.userName
                                ,password=passwd
                                ,port=self.port
                                ,pkey=priv_key_obj
                                ,look_for_keys=False
                                )
            print('Connection Successful.')
            if self.pub_key is not None:
                os.remove(pub_key_temp_file_name)      
            #Raises BadHostKeyException,AuthenticationException,SSHException,socket erro
        except (Exception) as e:
            return print('Error connecting to ssftp: {}'.format(e))
            sys.exit(0)
        
        stdin, stdout, stderr = ssh_client.exec_command("df -P --total | grep 'total' | awk \'{print $4}\'")
        stdout.channel.recv_exit_status()
        lines = stdout.readlines()
        total_target_size_string = [x.strip('\n') for x in lines]
        total_target_size = int(total_target_size_string[0])

        source_file_size = os.stat(source_file).st_size

        if source_file_size > total_target_size:
            print("Source file may be larger that the disk size of the sftp host.")
        
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(source_file,target_file)
        print('Payload Sent.')

        if meta_data is not False:
            meta_data = self.manifest_file_create(source_file, target_file, source_file_size)
            try:
                manifest_file = sftp_client.file(meta_data[0], 'a', -1)
                manifest_file.write(json.dumps(meta_data[1]))
                manifest_file.flush()
                print('Manifest Sent.')
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