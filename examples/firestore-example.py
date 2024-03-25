from google.cloud import firestore_v1
import os
from google.cloud.firestore_v1.base_query import FieldFilter,Or

db = ""


def update_existing_document(docID):
    # Get the reference to the collection
    collection_ref = db.collection('tasksCollection')

    # Get the document you want to update by its ID
    doc_ref = collection_ref.document(docID)

    # Update the document
    doc_ref.update({
        'new_field_data': 'some new'
    })

    # Get the document you want to update by its ID
    #doc_ref = collection_ref.document('your_document_id')

    # Update the document
    #doc_ref.update({
    #    'field_to_update': 'new_value'
    #})

def get_all_docs(collectionName):
    # Get the reference to the collection
    #collection_ref = db.collection(collectionName)
    docs = (
            db.collection(collectionName)
            .stream()
        )

    # Iterate over the documents and store their IDs and data in a list
    documents_list = []

    for doc in docs:
        doc_data = doc.to_dict()
        doc_data['id'] = doc.id
        doc_data['docData'] = doc._data

        #print(doc._data)
        documents_list.append(doc_data)

    # Print the list of documents
    for doc_data in documents_list:
        print(f"Document ID: {doc_data['id']}")
        print(f"Document Data: {doc_data['docData']}")
        print()


def get_document(collection_name, document_id):
    doc_ref = db.collection(collection_name).document(document_id)
    print(doc_ref)
    doc = doc_ref.get()
    print(doc)

    if doc.exists:
        return doc.to_dict()
    else:
        print(f"Document '{document_id}' not found in collection '{collection_name}'.")
        return None


def delete_document(collection_name, document_id):
    try:
        doc_ref = db.collection(collection_name).document(document_id)
        doc_ref.delete()
        print(f"Document with ID {document_id} deleted successfully.")

    except Exception as e:
        print(f"Error deleting document: {str(e)}")


def get_documents_with_status(collection_name, status_value):
    try:
        doc_ref = db.collection(collection_name)

        #make your query
        query = doc_ref.where(filter=FieldFilter("status", "==", status_value))

        #stream for results
        docs = query.stream()

        for doc in docs:
            data = doc.to_dict()
            print("Document data:", data)

    except Exception as e:
        print(f"Error retrieving documents: {str(e)}")

def get_different_status(collection_name, status_value1, status_value2):
    try:
        doc_ref = db.collection(collection_name)
        filter_todo = FieldFilter("status", "==", status_value1)
        filter_done = FieldFilter("status", "==", status_value2)

        # Create the union filter of the two filters (queries)
        or_filter = Or(filters=[filter_todo, filter_done])

        # Execute the query
        docs = doc_ref.where(filter=or_filter).stream()

        for doc in docs:
            data = doc.to_dict()
            print("Document data:", data)
    except Exception as e:
        print(f"Error retrieving documents: {str(e)}")


#READ DATA
get_all_docs("tasksCollection")
print(get_document('tasksCollection','flfWqunWhohtaJ7OpSzO' ))
get_documents_with_status("tasksCollection", "TODO")
get_different_status("tasksCollection", "TODO", "done")

#UPDATE DATA
update_existing_document("flfWqunWhohtaJ7OpSzO")

#DELETE DATA
delete_document('tasksCollection','flfWqunWhohtaJ7OpSzO')