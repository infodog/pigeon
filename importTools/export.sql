select hex(name), hex(content), isCompressed, isString from t_flexobject into outfile '/home/pigeonexport/mall_flextobject.txt';
select hex(listName), hex(value) from t_listband where isMeta=0 into outfile '/home/pigeonexport/mall_list.txt';
select hex(name), value from t_simpleatom into outfile '/home/pigeonexport/mall_atom.txt';
select hex(TableName), NextValue from t_ids into outfile '/home/pigeonexport/mall_id.txt';