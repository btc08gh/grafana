import { RelativeTimeRange, getDefaultRelativeTimeRange } from '@grafana/data';
import { dataSource as expressionDatasource } from 'app/features/expressions/ExpressionDatasource';
import {
  ExpressionDatasourceUID,
  ExpressionQuery,
  ExpressionQueryType,
  ReducerMode,
} from 'app/features/expressions/types';
import { defaultCondition } from 'app/features/expressions/utils/expressionTypes';
import { AlertQuery } from 'app/types/unified-alerting-dto';

import { mockDataQuery, mockReduceExpression, mockThresholdExpression } from '../../../mocks';

import {
  QueriesAndExpressionsState,
  addNewDataQuery,
  addNewExpression,
  duplicateQuery,
  optimizeReduceExpression,
  queriesAndExpressionsReducer,
  removeExpression,
  rewireExpressions,
  setDataQueries,
  updateExpression,
  updateExpressionRefId,
  updateExpressionTimeRange,
} from './reducer';

const reduceExpression: AlertQuery<ExpressionQuery> = {
  refId: 'B',
  queryType: 'expression',
  datasourceUid: '__expr__',
  model: {
    type: ExpressionQueryType.reduce,
    refId: 'B',
    settings: { mode: ReducerMode.Strict },
    expression: 'A',
  },
};
const thresholdExpression: AlertQuery<ExpressionQuery> = {
  refId: 'C',
  queryType: 'expression',
  datasourceUid: '__expr__',
  model: {
    type: ExpressionQueryType.threshold,
    refId: 'C',
  },
};

const ds1 = {
  id: 1,
  uid: 'c8eceabb-0275-4108-8f03-8f74faf4bf6d',
  type: 'prometheus',
  name: 'gdev-prometheus',
  meta: {
    alerting: true,
    info: {
      logos: {
        small: 'http://example.com/logo.png',
      },
    },
  },
  jsonData: {},
  access: 'proxy',
};

jest.mock('@grafana/runtime', () => ({
  ...jest.requireActual('@grafana/runtime'),
  getDataSourceSrv: () => ({
    getList: () => [ds1],
    getInstanceSettings: () => ds1,
  }),
}));

const alertQuery: AlertQuery = {
  refId: 'A',
  queryType: 'query',
  datasourceUid: 'abc123',
  model: {
    refId: 'A',
  },
};

const expressionQuery: AlertQuery = {
  datasourceUid: ExpressionDatasourceUID,
  model: expressionDatasource.newQuery({
    type: ExpressionQueryType.classic,
    conditions: [{ ...defaultCondition, query: { params: ['A'] } }],
    expression: '',
    refId: 'B',
  }),
  refId: 'B',
  queryType: '',
};

describe('Query and expressions reducer', () => {
  it('should return initial state', () => {
    expect(queriesAndExpressionsReducer(undefined, { type: '' })).toEqual({
      queries: [],
    });
  });

  it('should duplicate query', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, duplicateQuery(alertQuery));
    const newQuery = newState.queries.at(-1);
    expect(newState).toMatchSnapshot();
    expect(newQuery).toHaveProperty('relativeTimeRange', getDefaultRelativeTimeRange());
  });

  it('should duplicate query and copy time range', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery],
    };

    const customTimeRange = {
      from: -200,
      to: 800,
    };

    const query: AlertQuery = {
      ...initialState.queries[0],
      relativeTimeRange: customTimeRange,
    };

    const previousState: QueriesAndExpressionsState = {
      queries: [query],
    };

    const newState = queriesAndExpressionsReducer(previousState, duplicateQuery(query));
    const newQuery = newState.queries.at(-1);
    expect(newQuery).toHaveProperty('relativeTimeRange', customTimeRange);
  });

  it('should add query', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, addNewDataQuery());
    expect(newState.queries).toHaveLength(2);
    expect(newState).toMatchSnapshot();
  });

  it('should set data queries', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, setDataQueries([]));
    expect(newState.queries).toHaveLength(1);
    expect(newState).toMatchSnapshot();
  });

  it('should add a new expression', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, addNewExpression(ExpressionQueryType.math));
    expect(newState.queries).toHaveLength(2);
    expect(newState).toMatchSnapshot();
  });

  it('should remove an expression or alert query', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, expressionQuery],
    };

    const stateWithoutB = queriesAndExpressionsReducer(initialState, removeExpression('B'));
    expect(stateWithoutB.queries).toHaveLength(1);
    expect(stateWithoutB).toMatchSnapshot();

    const stateWithoutAOrB = queriesAndExpressionsReducer(stateWithoutB, removeExpression('A'));
    expect(stateWithoutAOrB.queries).toHaveLength(0);
  });

  it('should update an expression', () => {
    const newExpression: ExpressionQuery = {
      ...expressionQuery.model,
      type: ExpressionQueryType.math,
    };

    const initialState: QueriesAndExpressionsState = {
      queries: [expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, updateExpression(newExpression));
    expect(newState).toMatchSnapshot();
  });
  it('should use time range from data source when updating an expression', () => {
    const expressionQuery: AlertQuery = {
      refId: 'B',
      queryType: 'expression',
      datasourceUid: '__expr__',
      relativeTimeRange: { from: 900, to: 1000 },
      model: {
        queryType: 'query',
        datasource: '__expr__',
        refId: 'B',
        expression: 'C',
        type: ExpressionQueryType.classic,
        window: '10s',
      } as ExpressionQuery,
    };

    const expressionQuery2: AlertQuery = {
      refId: 'C',
      queryType: 'expression',
      datasourceUid: '__expr__',
      relativeTimeRange: { from: 1, to: 3 },
      model: {
        queryType: 'query',
        datasource: '__expr__',
        refId: 'C',
        expression: 'A',
        type: ExpressionQueryType.classic,
        window: '10s',
      } as ExpressionQuery,
    };

    const queryA: AlertQuery = {
      refId: 'A',
      relativeTimeRange: { from: 900, to: 1000 },
      datasourceUid: 'dsuid',
      model: { refId: 'A' },
      queryType: 'query',
    };
    const newExpression: ExpressionQuery = {
      ...expressionQuery2.model,
      type: ExpressionQueryType.resample,
    };

    const initialState: QueriesAndExpressionsState = {
      queries: [queryA, expressionQuery, expressionQuery2],
    };

    const newState = queriesAndExpressionsReducer(initialState, updateExpression(newExpression));
    expect(newState).toStrictEqual({
      queries: [
        {
          refId: 'A',
          relativeTimeRange: { from: 900, to: 1000 },
          datasourceUid: 'dsuid',
          model: { refId: 'A' },
          queryType: 'query',
        },
        {
          datasourceUid: '__expr__',
          relativeTimeRange: { from: 900, to: 1000 },
          model: {
            datasource: '__expr__',
            expression: 'C',
            queryType: 'query',
            refId: 'B',
            type: 'classic_conditions',
            window: '10s',
          },
          queryType: 'expression',
          refId: 'B',
        },
        {
          datasourceUid: '__expr__',
          relativeTimeRange: { from: 900, to: 1000 },
          model: {
            datasource: '__expr__',
            expression: 'A',
            queryType: 'query',
            refId: 'C',
            type: 'resample',
            window: '10s',
          },
          queryType: 'expression',
          refId: 'C',
        },
      ],
    });
  });

  it('Should update time range for all resample expressions that have this data source when dispatching updateExpressionTimeRange', () => {
    const expressionQuery: AlertQuery<ExpressionQuery> = {
      refId: 'B',
      queryType: 'expression',
      datasourceUid: '__expr__',
      model: {
        datasource: {
          type: '__expr__',
          uid: '__expr__',
        },
        queryType: 'query',
        refId: 'B',
        expression: 'A',
        type: ExpressionQueryType.resample,
        window: '10s',
      },
    };
    const customTimeRange: RelativeTimeRange = { from: 900, to: 1000 };

    const queryA: AlertQuery = {
      refId: 'A',
      relativeTimeRange: customTimeRange,
      datasourceUid: 'dsuid',
      model: { refId: 'A' },
      queryType: 'query',
    };

    const initialState: QueriesAndExpressionsState = {
      queries: [queryA, expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(initialState, updateExpressionTimeRange());
    expect(newState).toStrictEqual<{ queries: AlertQuery[] }>({
      queries: [
        {
          refId: 'A',
          relativeTimeRange: customTimeRange,
          datasourceUid: 'dsuid',
          model: { refId: 'A' },
          queryType: 'query',
        },
        {
          datasourceUid: '__expr__',
          model: {
            expression: 'A',
            datasource: {
              type: '__expr__',
              uid: '__expr__',
            },
            queryType: 'query',
            refId: 'B',
            type: ExpressionQueryType.resample,
            window: '10s',
          },
          queryType: 'expression',
          refId: 'B',
          relativeTimeRange: customTimeRange,
        },
      ],
    });
  });

  it('should update an expression refId and rewire expressions', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      updateExpressionRefId({
        oldRefId: 'A',
        newRefId: 'C',
      })
    );

    expect(newState).toMatchSnapshot();
  });

  it('should not update an expression when the refId exists', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      updateExpressionRefId({
        oldRefId: 'A',
        newRefId: 'B',
      })
    );

    expect(newState).toEqual(initialState);
  });

  it('should rewire expressions', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, expressionQuery],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      rewireExpressions({
        oldRefId: 'A',
        newRefId: 'C',
      })
    );

    expect(newState).toMatchSnapshot();
  });
  it('should remove first reducer', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, reduceExpression, thresholdExpression],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      optimizeReduceExpression({
        updatedQueries: [alertQuery],
        expressionQueries: [reduceExpression, thresholdExpression],
      })
    );
    expect(newState).toMatchSnapshot();
  });

  it('should remove reducer even if reducer is not the first expression', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, thresholdExpression, reduceExpression],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      optimizeReduceExpression({
        updatedQueries: [alertQuery],
        expressionQueries: [thresholdExpression, reduceExpression],
      })
    );
    expect(newState).toMatchSnapshot();
  });

  it('should not remove first reducer if reducer is not the second query', () => {
    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, alertQuery, reduceExpression, thresholdExpression],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      optimizeReduceExpression({
        updatedQueries: [alertQuery, alertQuery],
        expressionQueries: [reduceExpression, thresholdExpression],
      })
    );
    expect(newState).toEqual(initialState);
  });

  it('should add reduce expression if there is no reduce expression and the query is not instant', () => {
    const alertQuery: AlertQuery = {
      refId: 'A',
      queryType: 'query',
      datasourceUid: 'abc123',
      model: {
        refId: 'A',
        instant: false,
      },
    };

    const initialState: QueriesAndExpressionsState = {
      queries: [alertQuery, thresholdExpression],
    };

    const newState = queriesAndExpressionsReducer(
      initialState,
      optimizeReduceExpression({ updatedQueries: [alertQuery], expressionQueries: [thresholdExpression] })
    );
    expect(newState).toMatchSnapshot();
  });

  describe('dangling reference handling', () => {
    it('should clear expression reference when removing a data query that is referenced by a reduce expression', () => {
      const dataQuery = mockDataQuery({ refId: 'A' });
      const reduceExpr = mockReduceExpression({ refId: 'B', expression: 'A' });

      const initialState: QueriesAndExpressionsState = {
        queries: [dataQuery, reduceExpr],
      };

      // Remove the data query A
      const newState = queriesAndExpressionsReducer(initialState, removeExpression('A'));

      // The reduce expression should still exist but its reference should be cleared
      expect(newState.queries).toHaveLength(1);
      expect(newState.queries[0].refId).toBe('B');
      expect(newState.queries[0].model.expression).toBeNull();
    });

    it('should clear expression reference when removing a data query via setDataQueries', () => {
      const dataQuery = mockDataQuery({ refId: 'A' });
      const mathExpr: AlertQuery<ExpressionQuery> = {
        refId: 'C',
        queryType: 'expression',
        datasourceUid: ExpressionDatasourceUID,
        model: {
          refId: 'C',
          type: ExpressionQueryType.math,
          expression: '$A + 10', // references data query A
          datasource: {
            type: '__expr__',
            uid: '__expr__',
          },
        },
      };

      const initialState: QueriesAndExpressionsState = {
        queries: [dataQuery, mathExpr],
      };

      // Remove all data queries (simulating user deleting query A)
      const newState = queriesAndExpressionsReducer(initialState, setDataQueries([]));

      // The math expression should still exist but reference to A should be cleared
      expect(newState.queries).toHaveLength(1);
      expect(newState.queries[0].refId).toBe('C');
      // Math expressions with dangling refs should have them removed from the expression string
      expect(newState.queries[0].model.expression).not.toContain('$A');
    });

    it('should clear expression reference when removing an expression that is referenced by another expression', () => {
      const dataQuery = mockDataQuery({ refId: 'A' });
      const reduceExpr = mockReduceExpression({ refId: 'B', expression: 'A' });
      const thresholdExpr = mockThresholdExpression({ refId: 'C', expression: 'B' });

      const initialState: QueriesAndExpressionsState = {
        queries: [dataQuery, reduceExpr, thresholdExpr],
      };

      // Remove expression B which is referenced by C
      const newState = queriesAndExpressionsReducer(initialState, removeExpression('B'));

      // Both A and C should remain, but C's reference to B should be cleared
      expect(newState.queries).toHaveLength(2);
      expect(newState.queries.map((q) => q.refId)).toEqual(['A', 'C']);
      const thresholdQuery = newState.queries.find((q) => q.refId === 'C');
      expect(thresholdQuery?.model.expression).toBeNull();
    });
  });
});
