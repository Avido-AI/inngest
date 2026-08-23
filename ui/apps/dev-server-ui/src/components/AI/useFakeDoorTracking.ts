import { useCallback } from 'react';

export type FakeDoorFeature = 'scores' | 'experiments';

export type FakeDoorAction =
  | 'viewed'
  | 'prompt_copied'
  | 'example_copied'
  | 'docs_clicked'
  | 'cta_clicked';

export function useFakeDoorTracking(feature: FakeDoorFeature) {
  return useCallback(
    (_action: FakeDoorAction) => {
      void feature;
    },
    [feature],
  );
}
