var Riak   = require( '../lib/riak-dc.js' )
	, assert = require( 'assert' )
	, chai   = require( 'chai' )
	, cap    = require( 'chai-as-promised' )
	, nock   = require( 'nock' );

chai.use( cap );

it( 'Riak package initialised with test values', function () {
	assert.equal(Riak.init( 'localhost', '8098' ), 200)
} );

it( 'ping', function () {
	nock( 'http://localhost:8098', { 'Content-Type': 'application/json' } )
		.get( '/ping' )
		.reply( 200 );

	return Riak.ping().then( function (p) {
		assert( p, 'ping returns true' )
	} );
} );

it( 'get_buckets', function () {
	var buckets_rsp = { 'buckets': [ 'testing' ] };

	nock( 'http://localhost:8098', { 'Content-Type': 'application/json' } )
		.get( '/riak/?buckets=true' )
		.reply( 200, buckets_rsp );

	return Riak.get_buckets().then( function (buckets) {
		assert( buckets, 'defined return value' );
		assert.equal( buckets.length, 1, 'correct number of buckets' );
		assert.equal( buckets[0], 'testing', 'bucket 0 looks to be what we expected' );
	} );
} );

it( 'put_tuple (without key)', function () {
	var put_tuple_rsp = { 'json': 'true' };

	nock( 'http://localhost:8098'  )
		.post( '/riak/bucketname?returnbody=true' )
		.reply( 200, put_tuple_rsp, {
			'Content-Type': 'application/json',
			'Location': '/riak/bucketname/UQ47BRs5YSVepY2CEVrvuLY9EV7'
		} );
	return Riak.put_tuple( 'bucketname', put_tuple_rsp ).then( function ( key ) {
		assert( key, 'a key was returned' );
		assert.equal( key, 'UQ47BRs5YSVepY2CEVrvuLY9EV7', 'returned key is the key Riak supplied' );
	} );

} );

it( 'put_tuple (with key)', function () {
	var put_tuple_rsp = { 'json': 'true' };

	nock( 'http://localhost:8098' )
		.post( '/riak/bucketname/new_key_name?returnbody=true' )
		.reply( 200, put_tuple_rsp, {
			'Content-Type': 'application/json',
			'Location': '/riak/bucketname/new_key_name'
		} );

	return Riak.put_tuple( 'bucketname', put_tuple_rsp, 'new_key_name' ).then( function ( key ) {
		assert( key, 'a key was returned' );
		assert.equal( key, 'new_key_name', 'returned key is the key we supplied' );
	} );
} );

it( 'get_tuple (0-byte)', function () {
	nock( 'http://localhost:8098' )
		.get( '/riak/nodes/X0uzthV7wciJwNHjc2ymNqx4S5s' )
		.reply( 200, undefined, { 'Content-Type': 'application/json' } );

	return Riak.get_tuple( 'nodes', 'X0uzthV7wciJwNHjc2ymNqx4S5s' ).then( function ( tuple ) {
		if (tuple.constructor == Error) {
			// We have encountered a 0-byte tuple!
			assert( 1, 'returned error' )
		}
		else {
			assert( false, 'did not return an error' );
		}
	} );
} );

it( 'get_tuple', function () {
	var node_rsp = {
		'name':'first_node',
		'instance_id':'i-f7b8de1c',
		'availability_zone':'us-east-1a'
	};

	nock( 'http://localhost:8098' )
		.get( '/riak/nodes/X0uzthV7wciJwNHjc2ymNqx4S5s' )
		.reply( 200, node_rsp, { 'Content-Type': 'application/json' } );

	return Riak.get_tuple( 'nodes', 'X0uzthV7wciJwNHjc2ymNqx4S5s' ).then( function ( tuple ) {
		var parsed_tuple = JSON.parse(tuple);
		assert( tuple, 'tuple was returned' );
		assert( parsed_tuple, 'tuple parsed into json' );
		assert.deepEqual( parsed_tuple, node_rsp, 'returned value was not what was expected.' );
	} );

} );

it( 'get_keys', function () {
	// riak_long_reply{{{
	var riak_long_reply = {'props':{'name':'nodes','allow_mult':false,'basic_quorum':false,'big_vclock':50,'chash_keyfun':{'mod':'riak_core_util','fun':'chash_std_keyfun'},'dvv_enabled':false,'dw':'quorum','last_write_wins':false,'linkfun':{'mod':'riak_kv_wm_link_walker','fun':'mapreduce_linkfun'},'n_val':3,'notfound_ok':true,'old_vclock':86400,'postcommit':[],'pr':0,'precommit':[],'pw':0,'r':'quorum','rw':'quorum','small_vclock':50,'w':'quorum','young_vclock':20},'keys':['X0uzthV7wciJwNHjc2ymNqx4S5s']};

	// }}}

	nock( 'http://localhost:8098' )
		.get( '/riak/nodes?keys=true' )
		.reply( 200, riak_long_reply, { 'Content-Type': 'application/json' } );

	return Riak.get_keys( 'nodes' ).then( function (nodes) {
		assert( nodes, 'Riak returned something' );
		assert.deepEqual( nodes, [ 'X0uzthV7wciJwNHjc2ymNqx4S5s' ], 'list of nodes correct' );
	} );

} );

it( 'del_tuple', function () {
	nock( 'http://localhost:8098' )
		.delete( '/riak/nodes/X0uzthV7wciJwNHjc2ymNqx4S5s' )
		.reply( 204, '', { 'Content-Type': 'application/json' } );
	
	return Riak.del_tuple( 'nodes', 'X0uzthV7wciJwNHjc2ymNqx4S5s' ).then( function (r) {
		// So this should never really happen, but Riak doesn't really od much on
		// a successful delete.
		//
		if (r) {
			assert.notEqual( r, 'not found' );
		}
	} );
} );
