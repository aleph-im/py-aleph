********
Messages
********
All data transfered over the aleph.im network are aleph messages and represent the core of the Aleph networking model.

Message can be:

- sent and received on the REST or other API interfaces
- exchanged on the peer-to-peer network
- stored on the underlying chains

.. uml::

    @startuml
        entity Message {
            .. Message info ..
            *type : text
            one of: POST, AGGREGATE, STORE
            *channel : text
            (channel of the message, one application ideally has one channel)
            *time : timestamp
            .. Sender info ..
            *sender : text <<address>>
            *chain : text
            (chain of sender: NULS, NULS2, ETH, DOT, CSDK, SOL...)
            -- Content --
            *item_hash <<hash>>
            if IPFS: multihash of json serialization of content
            if inline: hash of item_content using hash_type (sha256 only for now)
            *item_content : text <<json>>
            mandatory if of inline type, json serialization of the message
            #item_type : text (optional)
            one of: IPFS, inline.
            default: IPFS if no item_content, inline if there is
            #hash_type : text (optional)
            default: sha256 (only supported value for now)
        }

        hide circle
    @enduml

Actual content sent by regular users can currently be of three types:

- AGGREGATE: a key-value storage specific to an address
- POST: unique data posts (unique data points, events)
- STORE: file storage


.. uml::
   
   @startuml
    object Message {
        ...
    }

    object Aggregate <<message content>> {
        key : text
        address : text <<address>>
        ~ content : object
        time : timestamp
    }

    object Post <<message content>> {
        type : text
        address : text <<address>>
        ~ content : object
        time : timestamp
    }

 object Store <<message content>> {
        address : text <<address>>
        item_type : same as Message.item_type (note: it does not support inline)
        Item_hash: same as Message.item_hash
        time : timestamp
    }



    Message ||--o| Aggregate
    Message ||--o| Post
    Message ||--o| Post
    Message ||--o| Store
   @enduml


Message types
=============

.. toctree::
   :maxdepth: 2

   aggregate
   post
