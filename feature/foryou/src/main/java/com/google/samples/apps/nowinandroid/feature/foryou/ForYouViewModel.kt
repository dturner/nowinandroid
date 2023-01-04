/*
 * Copyright 2022 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.samples.apps.nowinandroid.feature.foryou

import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.google.samples.apps.nowinandroid.core.data.repository.UserDataRepository
import com.google.samples.apps.nowinandroid.core.data.util.SyncStatusMonitor
import com.google.samples.apps.nowinandroid.core.domain.GetFollowableTopicsUseCase
import com.google.samples.apps.nowinandroid.core.domain.GetSaveableNewsResourcesUseCase
import com.google.samples.apps.nowinandroid.core.domain.model.SaveableNewsResource
import com.google.samples.apps.nowinandroid.core.model.data.NewsResourceType.Video
import com.google.samples.apps.nowinandroid.core.ui.NewsFeedUiState
import dagger.hilt.android.lifecycle.HiltViewModel
import javax.inject.Inject
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

@HiltViewModel
class ForYouViewModel @Inject constructor(
    syncStatusMonitor: SyncStatusMonitor,
    private val userDataRepository: UserDataRepository,
    private val getSaveableNewsResources: GetSaveableNewsResourcesUseCase,
    getFollowableTopics: GetFollowableTopicsUseCase
) : ViewModel() {

    private val shouldShowOnboarding: Flow<Boolean> =
        userDataRepository.userData.map { !it.shouldHideOnboarding }
    private var shouldOnlyShowVideos = MutableStateFlow(false)

    fun toggleShouldOnlyShowVideos(){
        shouldOnlyShowVideos.value = !shouldOnlyShowVideos.value
    }

    val isSyncing = syncStatusMonitor.isSyncing
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = false
        )

    val feedState: StateFlow<NewsFeedUiState> =
        combine (
            userDataRepository.userData,
            shouldOnlyShowVideos
        ) { userData, shouldOnlyShowVideosStream ->

            // If the user hasn't completed the onboarding and hasn't selected any interests
            // show an empty news list to clearly demonstrate that their selections affect the
            // news articles they will see.
            if (!userData.shouldHideOnboarding &&
                userData.followedTopics.isEmpty()
            ) {
                flowOf(NewsFeedUiState.Success(emptyList()))
            } else {
                getSaveableNewsResources(
                    filterTopicIds = userData.followedTopics
                ).map {
                    it.filter { newsItem ->
                        !shouldOnlyShowVideosStream || newsItem.newsResource.type == Video
                    }
                }
                .mapToFeedState()
            }
        }
            // Flatten the feed flows.
            // As the selected topics and topic state changes, this will cancel the old feed
            // monitoring and start the new one.
            .flatMapLatest { it }
            .stateIn(
                scope = viewModelScope,
                started = SharingStarted.WhileSubscribed(5_000),
                initialValue = NewsFeedUiState.Loading
            )

    val onboardingUiState: StateFlow<OnboardingUiState> =
        combine(
            shouldShowOnboarding,
            getFollowableTopics()
        ) { shouldShowOnboarding, topics ->
            if (shouldShowOnboarding) {
                OnboardingUiState.Shown(topics = topics)
            } else {
                OnboardingUiState.NotShown
            }
        }
            .stateIn(
                scope = viewModelScope,
                started = SharingStarted.WhileSubscribed(5_000),
                initialValue = OnboardingUiState.Loading
            )

    fun updateTopicSelection(topicId: String, isChecked: Boolean) {
        viewModelScope.launch {
            userDataRepository.toggleFollowedTopicId(topicId, isChecked)
        }
    }

    fun updateNewsResourceSaved(newsResourceId: String, isChecked: Boolean) {
        viewModelScope.launch {
            userDataRepository.updateNewsResourceBookmark(newsResourceId, isChecked)
        }
    }

    fun dismissOnboarding() {
        viewModelScope.launch {
            userDataRepository.setShouldHideOnboarding(true)
        }
    }
}

private fun Flow<List<SaveableNewsResource>>.mapToFeedState(): Flow<NewsFeedUiState> =
    map<List<SaveableNewsResource>, NewsFeedUiState>(NewsFeedUiState::Success)
        .onStart { emit(NewsFeedUiState.Loading) }
