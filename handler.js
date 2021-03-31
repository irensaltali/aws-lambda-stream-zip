'use strict';

const aws = require("aws-sdk");
aws.config.update( { region: "eu-central-1" } );
const s3 = new aws.S3();

const _archiver = require('archiver');
var allKeys=[];
exports.handler = async (_req, _ctx, _cb) => {
    console.log('start');
    
    var eventId = _req["Records"][0]["s3"]["object"]["key"].split('/')[0];
    var bucketName = _req["Records"][0]["s3"]["bucket"]["name"];
    var key = _req["Records"][0]["s3"]["object"]["key"];
    
    const dataSubData = await s3.getObject({  
      Bucket: bucketName,
      Key: key
    }).promise();
    
    var subscriptions = JSON.parse(dataSubData.Body.toString('utf-8'));
    console.log(subscriptions);
    
    await listAllKeys(bucketName, "/", null);
    await zipIt(bucketName,zipfileKey);
    CallAPI(eventId);
    
    Clean();
    _cb(null, { } );
};

function Clean(){
  allKeys=[];
}

const streamTo = (_bucket, _key) => {
	var stream = require('stream');
	var _pass = new stream.PassThrough();
	s3.upload( { Bucket: _bucket, Key: _key, Body: _pass }, (_err, _data) => { /*...Handle Errors Here*/ } );
	return _pass;
};


async function zipIt(bucketName,zipfileKey){
	    var _list = await Promise.all(allKeys.map(_key => new Promise((_resolve, _reject) => {
            let params = {Bucket: bucketName, Key: _key["from"]}
            var data = s3.getObject(params);
            _resolve( { data: data, name: _key["to"] } );
        }
    ))).catch(_err => { throw new Error(_err) } );

    await new Promise((_resolve, _reject) => { 
        var _myStream = streamTo(bucketName, zipfileKey);
        var _archive = _archiver('zip');
        _archive.on('error', err => { throw new Error(err); } );
        
        _myStream.on('close', _resolve);
        _myStream.on('end', _resolve);
        _myStream.on('error', _reject);
        
        _archive.pipe(_myStream);	
        _list.forEach(_itm => _archive.append(_itm.data.createReadStream(), { name: _itm.name } ) );
        _archive.finalize();
    }).catch(_err => { throw new Error(_err) } );
}

async function listAllKeys(bucketName, prefix, token)
{
  var listParams = { 
   Bucket: bucketName,
   Delimiter: '/',
   Prefix:prefix
  }
    
  if(token) listParams.ContinuationToken = token;

  s3.listObjectsV2(listParams, function(err, data){
      console.log(data.Contents);
      for(var i=0;i<data.Contents.length;i++){
          allKeys.push(data.Contents[i].Key);
      }

    if(data.IsTruncated)
      listAllKeys(data.NextContinuationToken);
    else
      zipIt();
  });
}

