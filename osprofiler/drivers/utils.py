import re

uuidhex = re.compile('[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')
uuid_without_dash = re.compile('[0-9a-fA-F]{32}')
numerical_id = re.compile('^[0-9]+$')
subst = {
    "bindings": "{hostname}",
    "os-keypairs": "{keypair_name}",
    "block-device-mapping": "{block_device}"
}


def clean_url(url):
    t = url.split('?')
    # Remove query string part
    target_url = t[0]
    # Split url per 'directory'
    t1 = target_url.split('/')
    for i in range(len(t1)):
        # In Openstack services, first part of url is never variable
        # - first token is '' as the URL begins with '/'
        # - second token is the real first part of url and is not variable in openstack
        if i < 2:
            continue
        collection_name = t1[i - 1]
        # We try to extract the singular of the collection
        if collection_name[-1] == 's':
            collection_name = collection_name[:-1]
        if uuidhex.match(t1[i]) or uuid_without_dash.match(t1[i]):
            t1[i] = "{" + collection_name + "_uuid}"
        if numerical_id.match(t1[i]):
            t1[i] = "{" + collection_name + "_id}"
        # Some exceptions when argument is not an uuid nor a numerical id
        t1[i] = subst.get(t1[i - 1], t1[i])

    target_url = "/".join(t1)

    return target_url