from configparser import ConfigParser

def config(filename="database.ini", section="postgresql"):
    #create parser
    parser = ConfigParser()
    #read config
    parser.read(filename)
    #empty db dict to store key:value
    db = {}
    #check section matches
    if parser.has_section(section):
        params = parser.items(section)
        #if matches loop through and build dict
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section:{section} is not found in the {filename} file.')
    print(db)

config()