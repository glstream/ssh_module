import gnupg

class gpg_encryption:    
    def __init__(self, key):        
        self.key = key      
   
    @staticmethod    
    def __get_gpg_client__():        
        gpg = gnupg.GPG()        
        return gpg
   
    def encrypt_data(self, source_file, target_file):        
        encryption_client = gpg_encryption.__get_gpg_client__()        
        import_result = encryption_client.import_keys(self.key)          
        with open(source_file, 'rb') as stream:  
            encrypted_data = encryption_client.encrypt_file(stream                                                  
                                                    , recipients=import_result.fingerprints[0]
                                                    , output=target_file
                                                    , always_trust=True)            
            print(encrypted_data.status)
            print(encrypted_data.ok)
            print(encrypted_data.stderr)

    def decrypt_data(self, source_file, target_file, passphrase=None):
        decryption_client = gpg_encryption.__get_gpg_client__()
        import_result = decryption_client.import_keys(self.key)
       
        stream = open(source_file, "rb")
        decrypted_data = decryption_client.decrypt_file(stream                                                  
                                                , output=target_file
                                                , passphrase=passphrase
                                                )            
        print(decrypted_data.status)
        print(decrypted_data.ok)
        print(decrypted_data.stderr)
       
    def decrypt_data_string(self, enc_string, passphrase='starbucks1'):
        encryption_client = gpg_encryption.__get_gpg_client__()
        import_result = encryption_client.import_keys(self.key)
        decrypted_data = encryption_client.decrypt(str(enc_string), passphrase=passphrase
                                                    )            
        print(decrypted_data)
