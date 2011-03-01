

 config = {_id: 'foo', members: [
                          {_id: 3, host: 'localhost:20013'},
                          {_id: 4, host: 'localhost:20014'}
                          ]
           }

rs.initiate(config);
