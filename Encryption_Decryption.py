# DBTITLE 1,Encryption & Decryption
import pyspark
import pyspark.sql
from cryptography.fernet import Fernet
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cryptography.fernet import Fernet
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Defined the data schema
data_schema = StructType([
    StructField("id", IntegerType()),
    StructField("Name", StringType())
])

# Defined the sample data 
data_1 = [(11, 'JOHN')]

# Creating dataframe
df = spark.createDataFrame(data=data_1, schema=data_schema)

# Creating a key for encryption / decryption
key = Fernet.generate_key()
masker = Fernet(key)

# Defining functions to encrypt and decrypt the data
def encrypt_Masker(data_to_encrypt):
    return masker.encrypt(data_to_encrypt.encode())

def decrypt_Masker(data_to_decrypt):
    return masker.decrypt(data_to_decrypt).decode()

# Convert DataFrame to JSON as inbuilt functions need bytes to encrypt
json_data = df.toJSON().collect()[0]

# Encrypted data
encrypted_data = encrypt_Masker(json_data)
print("Encrypted data : \n",encrypted_data)

# decrypted data
decrypted_data = decrypt_Masker(encrypted_data)
print("Decrypted data : \n",decrypted_data)

# - -------------------------------------  Output of programm ----------------
# Encrypted data : 
#  b'gAAAAABmJTsfymOvL8lMTiXJv6wmr4hcbIYBZ8H-4O5gmEJMKyRYS974k0hUH2IM0BROPg2zVB3XZRIkgs50fXGzojumWr2XIonJMv9fQTTT-aVYvC9WTWE='
# Decrypted data : 
#  {"id":11,"Name":"JOHN"}


