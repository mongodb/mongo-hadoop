

 config = {_id: 'foo', members: [
                          {_id: 3, host: 'localhost:20003'},
                          {_id: 4, host: 'localhost:20004'}
                          ]
           }

rs.initiate(config);
