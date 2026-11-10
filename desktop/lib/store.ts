import { configureStore, combineReducers } from '@reduxjs/toolkit';
import { persistStore, persistReducer } from 'redux-persist';
import createWebStorage from 'redux-persist/lib/storage/createWebStorage';

import runReducer from './features/runSlice';

const createNoopStorage = () => {
    return {
        getItem() {
            return Promise.resolve(null);
        },
        setItem(_key: string, value: unknown) {
            return Promise.resolve(value);
        },
        removeItem() {
            return Promise.resolve();
        },
    };
};

const storage = typeof window !== 'undefined' ? createWebStorage('local') : createNoopStorage();

// Nested persist config for run slice - only persist user preferences
// Exclude: events, messages, toolHistory (contain large base64 screenshots)
// run slice 的嵌套 persist 配置 —— 只持久化用户偏好
// 排除：events、messages、toolHistory（其中包含体积很大的 base64 截图）
const runPersistConfig = {
    key: 'run',
    storage,
    whitelist: [
        'selectedAgent',
        'researchStrategy',
    ],
};

const rootReducer = combineReducers({
    run: persistReducer(runPersistConfig, runReducer),
});

const persistConfig = {
    key: 'root',
    storage,
    blacklist: ['run'], // run has its own nested config
    // run 拥有自己的嵌套配置
};

const persistedReducer = persistReducer(persistConfig, rootReducer);

export const store = configureStore({
    reducer: persistedReducer,
    middleware: (getDefaultMiddleware) =>
        getDefaultMiddleware({
            serializableCheck: {
                // Ignore all redux-persist actions (they use non-serializable callbacks)
                // 忽略所有 redux-persist action（其中使用了不可序列化的回调）
                ignoredActions: [
                    'persist/PERSIST',
                    'persist/REHYDRATE',
                    'persist/PURGE',
                    'persist/FLUSH',
                    'persist/PAUSE',
                    'persist/REGISTER',
                ],
            },
        }),
});

export const persistor = persistStore(store);

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
