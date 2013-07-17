from bson.json_util import loads, dumps
import datetime
import mock
from mongows.mws.db import get_db
from mongows.mws.util import get_internal_coll_name, get_collection_names
from mongows.mws.views import ratelimit
from flask import session

from pymongo.errors import OperationFailure

from tests import MongoWSTestCase
from mongows.mws.views import CLIENTS_COLLECTION


class ViewsSetUpUnitTestCase(MongoWSTestCase):
    def test_create_mws_resource(self):
        url = '/mws/'
        rv = self.app.post(url)
        new_response_dict = loads(rv.data)
        self.assertIn('res_id', new_response_dict)
        res_id = new_response_dict['res_id']
        is_new = new_response_dict['is_new']
        self.assertIsNotNone(res_id)
        self.assertTrue(is_new)

        # check if res_id is unchanged
        rv = self.app.post(url)
        new_response_dict = loads(rv.data)
        new_res_id = new_response_dict['res_id']
        new_is_new = new_response_dict['is_new']
        self.assertIsNotNone(new_res_id)
        self.assertEqual(res_id, new_res_id)
        self.assertFalse(new_is_new)

    def test_create_mws_resource_new_session(self):
        url = '/mws/'
        rv = self.app.post(url)
        response_dict = loads(rv.data)
        self.assertIn('res_id', response_dict)
        res_id = response_dict['res_id']
        self.assertIsNotNone(res_id)

        with self.app.session_transaction() as sess:
            del sess['session_id']

        # check if res_id is unique
        rv = self.app.post(url)
        new_res_id = loads(rv.data)['res_id']
        self.assertIsNotNone(new_res_id)
        self.assertNotEqual(res_id, new_res_id)

    @mock.patch('mongows.mws.views.datetime')
    def test_keep_mws_alive(self, datetime_mock):
        first = datetime.datetime(2012, 7, 4)
        second = first + datetime.timedelta(days=1)
        datetime_mock.now.return_value = first
        db = get_db()

        # get a session to keep alive
        rv = self.app.post('/mws/')
        res_id = loads(rv.data)['res_id']

        with self.app.session_transaction() as sess:
            session_id = sess['session_id']
            res = db.clients.find({'res_id': res_id, 'session_id': session_id},
                                  {'timestamp': 1})
            _id = res[0]['_id']
            old_ts = res[0]['timestamp']
            self.assertEqual(old_ts, first)

            datetime_mock.now.return_value = second
            url = '/mws/' + res_id + '/keep-alive'
            rv = self.app.post(url)
            self.assertEqual(rv.status_code, 204)
            newres = db.clients.find({'_id': _id}, {'timestamp': 1})
            self.assertEqual(newres[0]['timestamp'], second)

    def test_ratelimit(self):
        rv = self.app.post('/mws/')
        self.res_id = loads(rv.data)['res_id']

        limit = self.real_app.config['RATELIMIT_QUOTA'] = 3

        def dummy():
            return ('', 204)

        with self.app.session_transaction() as client_sess:
            session_id = client_sess['session_id']

        with self.real_app.test_request_context():
            session['session_id'] = session_id
            for i in range(limit):
                self.assertEqual(ratelimit(dummy)(), ('', 204))

            self.assertEqual(ratelimit(dummy)()[1], 429)

    def test_ratelimit_no_session(self):
        def dummy():
            return ('', 204)

        with self.real_app.test_request_context():
            self.assertEqual(ratelimit(dummy)()[1], 401)


class DBTestCase(MongoWSTestCase):
    def setUp(self):
        super(DBTestCase, self).setUp()
        # Todo: For stuff that isn't checking authentication,
        # we probably don't want to rely on/use the authentication code
        rv = self.app.post('/mws/')
        response_dict = loads(rv.data)
        self.assertIn('res_id', response_dict)
        self.res_id = response_dict['res_id']
        self.assertIsNotNone(self.res_id)

        self.db = get_db()

        self.make_request_url = '/mws/%s/db/%%s' % (self.res_id)

    def _make_request(self, endpoint, data, method, expected_status):
        url = self.make_request_url % (endpoint)
        if data is not None:
            if isinstance(data, dict):
                data = dumps(
                    {k: v for k, v in data.iteritems() if v is not None}
                )
            else:
                data = dumps(data)
            if method == self.app.get:
                url = '%s?%s' % (url, data)
                data = None
        result = method(url, data=data, content_type='application/json')
        actual_status = result.status_code
        self.assertEqual(actual_status, expected_status,
                         "Expected request status to be %s, got %s instead" %
                         (expected_status, actual_status))
        result_dict = loads(result.data) if result.data else {}
        return result_dict

    def make_get_collection_names_request(self, expected_status=200):
        return self._make_request('getCollectionNames', None, self.app.get,
                                  expected_status)

    def make_db_drop_request(self, expected_status=204):
        self.make_request_url = '/mws/%s/db%%s' % (self.res_id)
        return self._make_request('', None, self.app.delete, expected_status)


class DBCollectionTestCase(DBTestCase):
    def setUp(self):
        super(DBCollectionTestCase, self).setUp()

        self.coll_name = 'test_collection'
        self.internal_coll_name = get_internal_coll_name(self.res_id,
                                                         self.coll_name)
        self.db = get_db()
        self.db_collection = self.db[self.internal_coll_name]

        self.make_request_url = '/mws/%s/db/%s/%%s' % \
                                (self.res_id, self.coll_name)

    def tearDown(self):
        super(DBCollectionTestCase, self).setUp()
        self.db_collection.drop()

    def make_find_request(self, query=None, projection=None, skip=None,
                          limit=None, expected_status=200):
        data = {
            'query': query,
            'projection': projection,
            'skip': skip,
            'limit': limit,
        }
        return self._make_request('find', data, self.app.get,
                                  expected_status)

    def make_insert_request(self, document, expected_status=204):
        data = {'document': document}
        return self._make_request('insert', data, self.app.post,
                                  expected_status)

    def make_remove_request(self, constraint, just_one=False,
                            expected_status=204):
        data = {'constraint': constraint, 'just_one': just_one}
        return self._make_request('remove', data, self.app.delete,
                                  expected_status)

    def make_update_request(self, query, update, upsert=False, multi=False,
                            expected_status=204):
        data = {
            'query': query,
            'update': update,
            'upsert': upsert,
            'multi': multi,
        }
        return self._make_request('update', data, self.app.put,
                                  expected_status)

    def make_aggregate_request(self, query=None, expected_status=200):
        return self._make_request('aggregate', query, self.app.get,
                                  expected_status)

    def make_drop_request(self, expected_status=204):
        return self._make_request('drop', None, self.app.delete,
                                  expected_status)

    def make_count_request(self, query=None, skip=None, limit=None,
                           expected_status=200):
        data = {'query': query, 'skip': skip, 'limit': limit}
        return self._make_request('count', data, self.app.get, expected_status)

    def make_ensure_index_request(self, keys, options=None,
                                  expected_status=204):
        data = {'keys': keys, 'options': options}
        return self._make_request('ensureIndex', data, self.app.post,
                                  expected_status)

    def make_reindex_request(self, expected_status=204):
        return self._make_request('reIndex', None, self.app.put,
                                  expected_status)

    def make_drop_index_request(self, name, expected_status=204):
        return self._make_request('dropIndex', {'name': name}, self.app.delete,
                                  expected_status)

    def make_drop_indexes_request(self, expected_status=204):
        return self._make_request('dropIndexes', None, self.app.delete,
                                  expected_status)

    def make_get_indexes_request(self, expected_status=200):
        return self._make_request('getIndexes', None, self.app.get,
                                  expected_status)

    def set_session_id(self, new_id):
        with self.app.session_transaction() as sess:
            sess['session_id'] = new_id


class FindUnitTestCase(DBCollectionTestCase):
    def test_find(self):
        query = {'name': 'mongo'}
        self.db_collection.insert(query)

        result = self.make_find_request(query)
        self.assertEqual(len(result), 1)
        self.assertEqual(result['result'][0]['name'], 'mongo')

    def test_skipping_results(self):
        self.db_collection.insert([{'val': i} for i in xrange(10)])

        response = self.make_find_request(query={}, skip=4)
        result = response['result']
        self.assertEqual(len(result), 6)
        values = [r['val'] for r in result]
        self.assertItemsEqual(values, range(4, 10))

    def test_limiting_results(self):
        self.db_collection.insert([{'val': i} for i in xrange(10)])

        response = self.make_find_request(query={}, limit=4)
        result = response['result']
        self.assertEqual(len(result), 4)
        values = [r['val'] for r in result]
        self.assertItemsEqual(values, range(4))

    def test_invalid_find_session(self):
        self.set_session_id('invalid_id')
        document = {'name': 'mongo'}
        result = self.make_find_request(document, expected_status=403)
        error = {
            'error': 403,
            'reason': 'Session error. User does not have access to res_id',
            'detail': '',
        }
        self.assertEqual(result, error)


class InsertUnitTestCase(DBCollectionTestCase):
    def test_simple_insert(self):
        document = {'name': 'Mongo'}
        self.make_insert_request(document)

        result = self.db_collection.find()
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0]['name'], 'Mongo')

    def test_multiple_document_insert(self):
        document = [{'name': 'Mongo'}, {'name': '10gen'}]
        self.make_insert_request(document)

        result = self.db_collection.find()
        self.assertEqual(result.count(), 2)
        names = [r['name'] for r in result]
        self.assertItemsEqual(names, ['Mongo', '10gen'])

    def test_invalid_insert_session(self):
        self.set_session_id('invalid_session')
        document = {'name': 'mongo'}
        self.make_insert_request(document, expected_status=403)

    def test_insert_quota(self):
        limit = self.real_app.config['QUOTA_COLLECTION_SIZE'] = 150
        self.make_insert_request([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ], expected_status=204)

        result = self.make_insert_request([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ], expected_status=403)

        error = {
            'error': 403,
            'reason': 'Collection size exceeded',
            'detail': ''
        }
        self.assertEqual(result, error)


class RemoveUnitTestCase(DBCollectionTestCase):
    def test_remove(self):
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])

        document = {'name': 'Mongo'}
        self.make_remove_request(document)

        result = self.db_collection.find()
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0]['name'], 'NotMongo')

    def test_remove_one(self):
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])

        document = {'name': 'Mongo'}
        self.make_remove_request(document, just_one=True)

        result = self.db_collection.find()
        names = [r['name'] for r in result]
        self.assertItemsEqual(names, ['Mongo', 'NotMongo'])

    def test_remove_requires_valid_res_id(self):
        self.set_session_id('invalid_session')
        self.make_remove_request({}, expected_status=403)


class UpdateUnitTestCase(DBCollectionTestCase):
    def test_upsert(self):
        result = self.db_collection.find({'name': 'Mongo'})
        self.assertEqual(result.count(), 0)

        self.make_update_request({}, {'name': 'Mongo'}, True)

        result = self.db_collection.find()
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0]['name'], 'Mongo')

    def test_update_one(self):
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])
        self.make_update_request({'name': 'Mongo'}, {'name': 'Mongo2'}, True)

        result = self.db_collection.find()
        names = [r['name'] for r in result]
        self.assertItemsEqual(names, ['Mongo', 'Mongo2', 'NotMongo'])

    def test_update_multi(self):
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])
        self.make_update_request(
            {'name': 'Mongo'},
            {'$set': {'name': 'Mongo2'}},
            False, True
        )

        result = self.db_collection.find()
        names = [r['name'] for r in result]
        self.assertItemsEqual(names, ['Mongo2', 'Mongo2', 'NotMongo'])

    def test_multi_upsert(self):
        # Does not exist - upsert
        self.make_update_request({}, {'$set': {'name': 'Mongo'}}, True, True)

        result = self.db_collection.find()
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0]['name'], 'Mongo')

        # Exists - multi-update
        self.db_collection.insert([{'name': 'Mongo'}, {'name': 'NotMongo'}])
        self.make_update_request(
            {'name': 'Mongo'},
            {'$set': {'name': 'Mongo2'}},
            True, True
        )

        result = self.db_collection.find()
        names = [r['name'] for r in result]
        self.assertItemsEqual(names, ['Mongo2', 'Mongo2', 'NotMongo'])

    def test_update_quota(self):
        limit = self.real_app.config['QUOTA_COLLECTION_SIZE'] = 500
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])
        self.make_update_request({'name': 'Mongo'}, {'name': 'Mongo2'},
                                 expected_status=204)

        result = self.make_update_request({'name': 'Mongo'},
                                          {'$set': {'a': list(range(50))}},
                                          expected_status=403)
        error = {
            'error': 403,
            'reason': 'Collection size exceeded',
            'detail': ''
        }
        self.assertEqual(result, error)

    def test_multi_update_quota(self):
        limit = self.real_app.config['QUOTA_COLLECTION_SIZE'] = 500
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])

        self.make_update_request({},
                                 {'$set': {'a': list(range(12))}},
                                 multi=False,
                                 expected_status=204)

        result = self.make_update_request({},
                                          {'$set': {'a': list(range(12))}},
                                          multi=True,
                                          expected_status=403)
        error = {
            'error': 403,
            'reason': 'Collection size exceeded',
            'detail': ''
        }
        self.assertEqual(result, error)


class AggregateUnitTestCase(DBCollectionTestCase):
    def test_aggregate(self):
        for i in range(6):
            self.db_collection.insert({'val': i})

        query = [
            {'$match': {'val': {'$lt': 5}}},
            {'$sort': {'val': -1}},
            {'$skip': 1},
            {'$limit': 2}
        ]
        self.db_collection.aggregate(query)

        result = self.make_aggregate_request(query)
        self.assertEqual(result['ok'], 1)
        result = result['result']
        self.assertEqual(len(result), 2)
        self.assertEqual([x['val'] for x in result], [3, 2])

    def test_invalid_query(self):
        result = self.make_aggregate_request({}, expected_status=400)
        self.assertEqual(result['error'], 400)

        with self.assertRaises(OperationFailure) as cm:
            self.db_collection.aggregate({})
        self.assertEqual(cm.exception.message, result['reason'])

    def test_invalid_find_session(self):
        self.set_session_id('invalid_id')
        query = [{'$match': {'val': {'$lt': 5}}}]
        result = self.make_aggregate_request(query, expected_status=403)
        error = {
            'error': 403,
            'reason': 'Session error. User does not have access to res_id',
            'detail': '',
        }
        self.assertEqual(result, error)


class CountTestCase(DBCollectionTestCase):
    def test_get_query_count(self):
        self.db_collection.insert([{'n': i} for i in xrange(10)])
        response = self.make_count_request({'n': {'$gt': 5}})
        self.assertEqual(response['count'], 4)

        self.db_collection.insert([{'n': i} for i in xrange(10)])
        response = self.make_count_request({'n': {'$gt': 4}})
        self.assertEqual(response['count'], 10)

    def test_uses_skip_and_limit_info(self):
        self.db_collection.insert([{'n': i} for i in xrange(10)])
        response = self.make_count_request({}, skip=0, limit=1)
        self.assertEqual(response['count'], 1)

        response = self.make_count_request({}, skip=8, limit=0)
        self.assertEqual(response['count'], 2)


class EnsureIndexTestCase(DBCollectionTestCase):
    def test_single_index(self):
        self.db_collection.insert([{'key': 1}, {'key': 2}, {'key': 3}])
        self.assertEqual(len(self.db_collection.index_information()), 1)
        self.make_ensure_index_request({'key': 1})
        self.assertEqual(len(self.db_collection.index_information()), 2)

    def test_compound_index(self):
        self.db_collection.insert([{'key': 1}, {'key': 2}, {'key': 3},
                                   {'key2': 4}, {'key2': 5}, {'key2': 6}])
        self.assertEqual(len(self.db_collection.index_information()), 1)
        self.make_ensure_index_request({'key': 1, 'key2': '2d'})
        self.assertEqual(len(self.db_collection.index_information()), 2)

    def test_options(self):
        self.db_collection.insert([{'key': 1}, {'key': 2}, {'key': 3}])
        self.assertEqual(len(self.db_collection.index_information()), 1)
        self.make_ensure_index_request({'key': 1}, {
            'background': True,
            'unique': True,
            'name': 'idx',
            'dropDups': True,
            'sparse': True,
            'expireAfterSeconds': 60
        })
        info = self.db_collection.index_information()
        self.assertEqual(len(info), 2)
        self.assertItemsEqual(info['idx'], {
            'background': True,
            'unique': True,
            'dropDups': True,
            'sparse': True,
            'expireAfterSeconds': 60,
            'key': [('key', 1)],
            'v': 1
        })


class ReIndexTestCase(DBCollectionTestCase):
    @mock.patch('pymongo.collection.Collection.reindex')
    def test_reindex(self, reindex_mock):
        self.db_collection.ensure_index('a', 1)
        self.make_reindex_request()
        self.assertTrue(reindex_mock.called)
        self.assertEqual(reindex_mock.call_args_list, [()])


class DropIndexTestCase(DBCollectionTestCase):
    def test_drop_index(self):
        self.db_collection.ensure_index('a', 1, name='idx')
        self.assertTrue('idx' in self.db_collection.index_information())
        self.make_drop_index_request('idx')
        self.assertFalse('idx' in self.db_collection.index_information())

        self.db_collection.ensure_index('a', 1, name='idx1')
        self.db_collection.ensure_index('b', 1, name='idx2')
        info = self.db_collection.index_information()
        self.assertTrue('idx1' in info)
        self.assertTrue('idx2' in info)
        self.make_drop_index_request('idx1')
        info = self.db_collection.index_information()
        self.assertFalse('idx1' in info)
        self.assertTrue('idx2' in info)


class DropIndexesTestCase(DBCollectionTestCase):
    def test_drop_indexes(self):
        self.db_collection.ensure_index('a', 1, name='idx')
        self.assertTrue('idx' in self.db_collection.index_information())
        self.make_drop_indexes_request()
        self.assertFalse('idx' in self.db_collection.index_information())

        self.db_collection.ensure_index('a', 1, name='idx1')
        self.db_collection.ensure_index('b', 1, name='idx2')
        info = self.db_collection.index_information()
        self.assertTrue('idx1' in info)
        self.assertTrue('idx2' in info)
        self.make_drop_indexes_request()
        info = self.db_collection.index_information()
        self.assertFalse('idx1' in info)
        self.assertFalse('idx2' in info)


class GetIndexesTestCase(DBCollectionTestCase):
    def test_get_indexes(self):
        self.db_collection.ensure_index('a', 1, name='idx')
        result = self.make_get_indexes_request()
        self.assertEqual(result, [
            {
                'ns': self.db_collection.full_name,
                'name': '_id_',
                'key': {'_id': 1},
                'v': 1
            },
            {
                'ns': self.db_collection.full_name,
                'name': 'idx',
                'key': {'a': 1},
                'v': 1
            }
        ])


class DropUnitTestCase(DBCollectionTestCase):
    def test_drop(self):
        self.db_collection.insert([
            {'name': 'Mongo'}, {'name': 'Mongo'}, {'name': 'NotMongo'}
        ])

        result = self.db_collection.find()
        self.assertEqual(result.count(), 3)

        self.make_drop_request()

        result = self.db_collection.find()
        self.assertEqual(result.count(), 0)

        self.assertNotIn(self.internal_coll_name, self.db.collection_names())


class GetCollectionNamesUnitTestCase(DBTestCase):
    def test_get_collection_names(self):
        result = self.make_get_collection_names_request()['result']
        self.assertEqual(result, [])

        self.db[CLIENTS_COLLECTION].update({'res_id': self.res_id},
                                           {'$push': {'collections': 'test'}})
        result = self.make_get_collection_names_request()['result']
        self.assertEqual(result, ['test'])

    def test_invalid_session(self):
        with self.app.session_transaction() as sess:
            sess['session_id'] = 'invalid session'
        result = self.make_get_collection_names_request(expected_status=403)
        error = {
            'error': 403,
            'reason': 'Session error. User does not have access to res_id',
            'detail': '',
        }
        self.assertEqual(result, error)

    def test_resid_isolation(self):
        self.db[CLIENTS_COLLECTION].update({'res_id': self.res_id},
                                           {'$push': {'collections': 'test'}})

        result = self.make_get_collection_names_request()['result']
        self.assertEqual(result, ['test'])

        with self.app.session_transaction() as sess:
            del sess['session_id']
        new_resid = loads(self.app.post('/mws/').data)['res_id']
        self.assertNotEqual(self.res_id, new_resid)
        self.db[CLIENTS_COLLECTION].update({'res_id': new_resid},
                                           {'$push': {'collections': 'test2'}})

        self.make_request_url = '/mws/%s/db/%%s' % (new_resid)
        result = self.make_get_collection_names_request()['result']
        self.assertEqual(result, ['test2'])


class DropDBUnitTestCase(DBTestCase):
    def test_drop_db(self):
        testdoc = {'name': 'Mongo'}
        colls = ['a', 'b', 'c']
        self.db[CLIENTS_COLLECTION].update({'res_id': self.res_id},
                                           {'$addToSet':
                                           {'collections': {'$each': colls}}})
        colls = [get_internal_coll_name(self.res_id, c) for c in colls]
        for c in colls:
            self.db[c].insert(testdoc)

        actual_colls = self.db.collection_names()
        for c in colls:
            self.assertIn(c, actual_colls)

        self.make_db_drop_request()

        actual_colls = self.db.collection_names()
        for c in colls:
            self.assertNotIn(c, actual_colls)

        self.assertItemsEqual(get_collection_names(self.res_id), [])


class IntegrationTestCase(DBCollectionTestCase):
    def test_insert_find(self):
        document = {'name': 'mongo'}
        self.make_insert_request(document)

        result = self.make_find_request(document)
        self.assertDictContainsSubset(document, result['result'][0])
