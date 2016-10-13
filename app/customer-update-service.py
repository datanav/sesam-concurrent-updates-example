import os

from flask import Flask, request, jsonify
import sesamclient
from sesamclient import entity_json

app = Flask(__name__)

sesam_url = os.getenv('SESAM_URL', 'http://localhost:9042/api/')

sesam = sesamclient.Connection(sesamapi_base_url=sesam_url)


@app.route('/<dataset>/<entity_id>', methods=['DELETE'])
def delete_entity(dataset, entity_id):
    entity = sesam.get_dataset(dataset).get_entity(entity_id)
    entity["_deleted"] = True
    sesam.get_pipe(dataset).post_entities([entity])
    return jsonify(entity)


@app.route('/<dataset>/<entity_id>', methods=['PUT'])
def put_entity(dataset, entity_id):
    entity = request.json
    entity["_id"] = entity_id
    existing_entity = sesam.get_dataset(dataset).get_entity(entity_id)
    if existing_entity:
        # entity already exists, lets see if the request contains a version number
        if entity.get("_updated", None) is None:
            # nope, request does not contain version info, we can't resolve this
            return jsonify({
                "message": "Entity already exists, you need to GET the resource and modify it before you post it back",
                "ours": existing_entity,
                "yours": entity
            }), 409
        if entity["_updated"] == existing_entity["_updated"]:
            # yes, request contains a version and nothing has changed in between
            entity = entity
        else:
            # hmm, let's see if Sesam still has the older version this update is based on
            older_entity = get_entity_version(dataset, entity_id, entity["_updated"])
            if not older_entity:
                # darn, log compaction must have kicked in and removed the old version, we can't fix this
                return jsonify({
                    "message": "Sorry, entity has been updated and we can't find the older entity you have based the update on",
                    "ours": existing_entity,
                    "yours": entity
                }), 409
            try:
                # let's see if we can safely merge the changes
                entity = merge3_dicts(entity, older_entity, existing_entity)
            except MergeConflictException as e:
                # ouch, couldn't resolve this automatically
                return jsonify({
                    "message": "Sorry, there has been a conflict. Can't perform automatic merge.",
                    "ours": existing_entity,
                    "base": older_entity,
                    "yours": entity,
                    "conflicts": e.conflicts,
                }), 409
        # potential race condition (to be fixed in IS-3396)
        sesam.get_pipe(dataset).post_entities([entity])
    return get_entity(dataset, entity_id)


@app.route('/<dataset>/<entity_id>', methods=['GET'])
def get_entity(dataset, entity_id):
    return jsonify(sesam.get_dataset(dataset).get_entity(entity_id))


def get_entity_version(dataset_id, entity_id, offset):
    # soon to be part of the sesamclient (IS-3393)
    url = sesam.get_dataset_entity_url(dataset_id, entity_id) + "?offset=" + str(offset)
    response = sesam.do_get_request(url, allowable_response_status_codes=[200, 404])
    if response.status_code == 404:
        # No such element.
        return None
    return entity_json.parse_entity(response.text)


class MergeConflictException(Exception):
    def __init__(self, conflicts):
        self.conflicts = conflicts
    pass


def merge3_dicts(yours, base, ours):
    # simple implementation of a three way merge
    target = {}
    conflicts = []
    keys = set(list(yours.keys()) + list(base.keys()) + list(ours.keys()))
    # iterate all keys
    for key in keys:
        your_value = yours[key]
        base_value = base[key]
        our_value = ours[key]
        if your_value == base_value:
            # you have not changed that attribute, we use our value
            target[key] = our_value
        elif base_value == our_value:
            # we have not changed that attribute, we use your value
            target[key] = your_value
        elif your_value == our_value:
            # both we and you have done the same change, we use that value
            target[key] = our_value
        else:
            # oops, both we and you have changed the value to different values, we give up
            conflicts.append({
                "_key": key,
                "your_value": your_value,
                "our_value": our_value,
                "base_value": base_value,
            })
    if conflicts:
        raise MergeConflictException(conflicts)
    return target


if __name__ == '__main__':
    app.run(host="0.0.0.0")
