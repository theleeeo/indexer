package tests

import (
	"indexer/gen/index/v1"
	"indexer/gen/search/v1"

	"google.golang.org/protobuf/types/known/structpb"
)

func (t *TestSuite) Test_Resource_CRUD_OneIndex() {
	t.Run("create resources", func() {
		err := t.app.RegisterCreate(t.T().Context(), &index.CreatePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"field1": {
						Kind: &structpb.Value_StringValue{StringValue: "value1"},
					},
					// "field2": {
					// 	Kind: &structpb.Value_BoolValue{BoolValue: true},
					// },
				},
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		err = t.app.RegisterCreate(t.T().Context(), &index.CreatePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "2",
			},
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"field1": {
						Kind: &structpb.Value_StringValue{StringValue: "value2"},
					},
					// "field2": {
					// 	Kind: &structpb.Value_BoolValue{BoolValue: true},
					// },
				},
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())
	})

	t.Run("no query or filters", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 2)
		t.Require().Equal("1", resp.Hits[0].Id)
		t.Require().Equal("2", resp.Hits[1].Id)
	})

	t.Run("with query, string value", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "value1",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	// TODO: We cant allow a string query on bool values
	// t.Run("with query, bool value", func() {
	// 	resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{
	// 		Resource: "a",
	// 		Query:    "true",
	// 	})
	// 	t.Require().NoError(err)
	// 	t.Require().Len(resp.Hits, 2)
	// 	t.Require().Equal("1", resp.Hits[0].Id)
	// 	t.Require().Equal("2", resp.Hits[1].Id)
	// })

	t.Run("with query, no matches", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "false",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})

	t.Run("update existing resource", func() {
		err := t.app.RegisterUpdate(t.T().Context(), &index.UpdatePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"field1": {
						Kind: &structpb.Value_StringValue{StringValue: "updated_value"},
					},
					// "field2": {
					// 	Kind: &structpb.Value_BoolValue{BoolValue: false},
					// },
				},
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "updated_value",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	t.Run("delete resource", func() {
		err := t.app.RegisterDelete(t.T().Context(), &index.DeletePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].Id)
	})

	t.Run("delete non-existing resource", func() {
		err := t.app.RegisterDelete(t.T().Context(), &index.DeletePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "non_existing_id",
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())
	})
}

func (t *TestSuite) Test_Resource_CRUD_MultipleIndices() {
	t.Run("create resources in different indices", func() {
		err := t.app.RegisterCreate(t.T().Context(), &index.CreatePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
			Data: &structpb.Struct{},
		})
		t.Require().NoError(err)

		err = t.app.RegisterCreate(t.T().Context(), &index.CreatePayload{
			Resource: &index.Resource{
				Type: "b",
				Id:   "2",
			},
			Data: &structpb.Struct{},
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("search in index a", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	t.Run("search in index b", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].Id)
	})

	t.Run("search in non existing index", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "c"})
		t.Require().EqualError(err, "unknown resource")
		t.Require().Nil(resp)
	})

	t.Run("delete resources", func() {
		err := t.app.RegisterDelete(t.T().Context(), &index.DeletePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
		})
		t.Require().NoError(err)

		err = t.app.RegisterDelete(t.T().Context(), &index.DeletePayload{
			Resource: &index.Resource{
				Type: "b",
				Id:   "2",
			},
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("verify deletions", func() {
		resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)

		resp, err = t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})
}

func (t *TestSuite) Test_Create_WithRelation() {
	t.Run("create resource with relation", func() {
		err := t.app.RegisterCreate(t.T().Context(), &index.CreatePayload{
			Resource: &index.Resource{
				Type: "a",
				Id:   "1",
			},
			Data: &structpb.Struct{},
			Relations: []*index.Relation{
				{
					Resource: &index.Resource{
						Type: "b",
						Id:   "1",
					},
				},
			},
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		t.Run("verify relation", func() {
			resp, err := t.app.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
			t.Require().NoError(err)
			t.Require().Equal("1", resp.Hits[0].Id)
			relations := resp.Hits[0].Source.Fields["b"].GetListValue().GetValues()
			t.Require().Len(relations, 1)
			t.Require().Equal("1", relations[0].GetStructValue().Fields["id"].GetStringValue())
		})
	})
}
