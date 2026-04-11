import { createApi } from '@reduxjs/toolkit/query/react';
import { graphqlRequestBaseQuery } from '@rtk-query/graphql-request-base-query';
import { GraphQLClient } from 'graphql-request';

const graphQLEndpoint = new URL(
  '/v0/gql',
  import.meta.env.VITE_PUBLIC_API_BASE_URL || window.location.origin,
);

export const client = new GraphQLClient(graphQLEndpoint.toString());

export const api = createApi({
  baseQuery: graphqlRequestBaseQuery({
    client,
  }),
  endpoints: () => ({}),
});
